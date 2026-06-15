(ns simplemono.eventstore.s3-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.string]
            [simplemono.eventstore.protocols :as p]
            [simplemono.eventstore.s3 :as s3])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.util.zip GZIPInputStream)
           (software.amazon.awssdk.core ResponseInputStream)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                      GetObjectResponse
                                                      HeadObjectRequest
                                                      HeadObjectResponse
                                                      ListObjectsV2Request
                                                      ListObjectsV2Response
                                                      NoSuchKeyException
                                                      PutObjectRequest
                                                      PutObjectResponse
                                                      S3Exception
                                                      S3Object)))

(deftest inverted-commit-numbers-sort-newest-first
  (is (= "9223372036854775807" (s3/format-inverted-commit-number 0)))
  (is (= "9223372036854775806" (s3/format-inverted-commit-number 1)))
  (is (= "9223372036854775805" (s3/format-inverted-commit-number 2)))
  (is (= "0000000000000000000"
         (s3/format-inverted-commit-number Long/MAX_VALUE)))
  (is (= (map s3/format-inverted-commit-number [4 3 2 1 0])
         (sort (map s3/format-inverted-commit-number [0 1 2 3 4])))))

(deftest inverted-commit-number-roundtrip
  (doseq [n [0 1 2 10 100 Long/MAX_VALUE]]
    (is (= n (s3/parse-inverted-commit-number
              (s3/format-inverted-commit-number n))))))

(deftest commit-keys-use-normalized-prefixes
  (is (= "commits/9223372036854775807"
         (s3/commit-key "" 0)))
  (is (= "streams/acme/commits/9223372036854775807"
         (s3/commit-key "/streams/acme/" 0)))
  (is (= "streams/acme/commits/9223372036854775806"
         (s3/commit-key "streams/acme" 1))))

(def commit-0
  {:commit/number 0
   :commit/id #uuid "00000000-0000-0000-0000-000000000001"
   :commit/timestamp #inst "2026-01-01T00:00:00.000-00:00"
   :commit/events [{:event/id #uuid "00000000-0000-0000-0000-000000000101"
                    :event/type :example/created}]})

(def commit-1
  {:commit/number 1
   :commit/id #uuid "00000000-0000-0000-0000-000000000002"
   :commit/timestamp #inst "2026-01-01T00:00:01.000-00:00"
   :commit/events [{:event/id #uuid "00000000-0000-0000-0000-000000000102"
                    :event/type :example/updated}]})

(defn- no-such-key
  []
  (-> (NoSuchKeyException/builder)
      (.statusCode 404)
      (.message "No such key")
      (.build)))

(defn- precondition-failed
  []
  (-> (S3Exception/builder)
      (.statusCode 412)
      (.message "Precondition Failed")
      (.build)))

(defn- request-body->bytes
  [body]
  (with-open [in (.newStream (.contentStreamProvider body))]
    (let [out (ByteArrayOutputStream.)]
      (.transferTo in out)
      (.toByteArray out))))

(defn- gunzip-string
  [bytes]
  (with-open [gzip (GZIPInputStream. (ByteArrayInputStream. bytes))]
    (slurp gzip :encoding "UTF-8")))

(defn- request-headers
  [request]
  (some-> request .overrideConfiguration (.orElse nil) .headers))

(defn- if-none-match-star?
  [request]
  (boolean (some #{"*"} (get (request-headers request) "If-None-Match"))))

(defn- fake-s3-client
  [objects]
  (reify S3Client
    (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
      (let [key (.key request)]
        (when (and (if-none-match-star? request)
                   (contains? @objects key))
          (throw (precondition-failed)))
        (swap! objects assoc key (request-body->bytes body))
        (-> (PutObjectResponse/builder) (.build))))

    (^ResponseInputStream getObject [_ ^GetObjectRequest request]
      (if-let [body (get @objects (.key request))]
        (ResponseInputStream.
         (-> (GetObjectResponse/builder) (.build))
         (ByteArrayInputStream. body))
        (throw (no-such-key))))

    (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
      (if (contains? @objects (.key request))
        (-> (HeadObjectResponse/builder) (.build))
        (throw (no-such-key))))

    (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request request]
      (let [prefix (.prefix request)
            objects (->> @objects
                         keys
                         sort
                         (filter #(clojure.string/starts-with? % prefix))
                         (map #(-> (S3Object/builder) (.key %) (.build)))
                         vec)]
        (-> (ListObjectsV2Response/builder)
            (.contents objects)
            (.isTruncated false)
            (.build))))))

(deftest extra-headers-are-added-to-s3-requests
  (let [seen (atom [])
        objects (atom {})
        client (reify S3Client
                 (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
                   (swap! seen conj [:head (request-headers request)])
                   (if (contains? @objects (.key request))
                     (-> (HeadObjectResponse/builder) (.build))
                     (throw (no-such-key))))
                 (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
                   (swap! seen conj [:put (request-headers request)])
                   (swap! objects assoc (.key request) (request-body->bytes body))
                   (-> (PutObjectResponse/builder) (.build))))
        store (s3/store {:client client
                         :bucket "events"
                         :prefix "streams/acme"
                         :headers {"X-Tigris-Consistent" "true"}})]
    (is (true? (p/try-append! store 0 commit-0)))
    (is (true? (p/try-append! store 1 commit-1)))
    (is (= [[:put {"X-Tigris-Consistent" ["true"]
                   "If-None-Match" ["*"]}]
            [:head {"X-Tigris-Consistent" ["true"]}]
            [:put {"X-Tigris-Consistent" ["true"]
                   "If-None-Match" ["*"]}]]
           @seen))))

(deftest s3-store-is-create-only-and-gap-free
  (let [objects (atom (sorted-map))
        store (s3/store {:client (fake-s3-client objects)
                         :bucket "events"
                         :prefix "streams/acme"})]
    (is (nil? (p/latest-commit-number store)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Append would create a gap"
                          (p/try-append! store 1 commit-1)))
    (is (true? (p/try-append! store 0 commit-0)))
    (is (false? (p/try-append! store 0 commit-0)))
    (is (true? (p/try-append! store 1 commit-1)))
    (is (= 1 (p/latest-commit-number store)))
    (is (= commit-0 (p/get-commit store 0)))
    (is (= commit-1 (p/get-commit store 1)))
    (is (nil? (p/get-commit store 2)))
    (is (= (pr-str commit-0)
           (gunzip-string (get @objects "streams/acme/commits/9223372036854775807"))))
    (is (= ["streams/acme/commits/9223372036854775806"
            "streams/acme/commits/9223372036854775807"]
           (keys @objects)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.eventstore.s3-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
