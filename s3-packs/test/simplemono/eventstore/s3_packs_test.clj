(ns simplemono.eventstore.s3-packs-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string]
            [clojure.test :refer [deftest is run-tests]]
            [simplemono.eventstore.protocols :as p]
            [simplemono.eventstore.s3 :as s3]
            [simplemono.eventstore.s3-packs :as packs])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.nio.charset StandardCharsets)
           (java.util.zip GZIPInputStream GZIPOutputStream)
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

(def prefix "streams/acme")

(defn- commit
  [n]
  {:commit/number n
   :commit/id (java.util.UUID/nameUUIDFromBytes
               (.getBytes (str "commit-" n) StandardCharsets/UTF_8))
   :commit/events [{:event/kind :example/happened
                    :event/n n}]})

(defn- utf8
  [s]
  (.getBytes s StandardCharsets/UTF_8))

(defn- gzip-bytes
  [s]
  (let [out (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. out)]
      (.write gzip (utf8 s)))
    (.toByteArray out)))

(defn- seed-commits!
  [objects n]
  (swap! objects into
         (for [commit-number (range n)]
           [(s3/commit-key prefix commit-number)
            (gzip-bytes (pr-str (commit commit-number)))])))

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
      (io/copy in out)
      (.toByteArray out))))

(defn- request-headers
  [request]
  (some-> request .overrideConfiguration (.orElse nil) .headers))

(defn- if-none-match-star?
  [request]
  (boolean (some #{"*"} (get (request-headers request) "If-None-Match"))))

(defn- fake-s3-client
  [objects requests]
  (reify S3Client
    (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
      (let [key (.key request)]
        (swap! requests conj [:put key (request-headers request)])
        (when (and (if-none-match-star? request)
                   (contains? @objects key))
          (throw (precondition-failed)))
        (swap! objects assoc key (request-body->bytes body))
        (-> (PutObjectResponse/builder) (.build))))

    (^ResponseInputStream getObject [_ ^GetObjectRequest request]
      (let [key (.key request)]
        (swap! requests conj [:get key (request-headers request)])
        (if-let [body (get @objects key)]
          (ResponseInputStream.
           (-> (GetObjectResponse/builder) (.build))
           (ByteArrayInputStream. body))
          (throw (no-such-key)))))

    (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
      (let [key (.key request)]
        (swap! requests conj [:head key (request-headers request)])
        (if (contains? @objects key)
          (-> (HeadObjectResponse/builder) (.build))
          (throw (no-such-key)))))

    (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request request]
      (let [prefix (.prefix request)
            objects (->> @objects
                         keys
                         sort
                         (filter #(clojure.string/starts-with? % prefix))
                         (map #(-> (S3Object/builder) (.key %) (.build)))
                         vec)]
        (swap! requests conj [:list prefix (request-headers request)])
        (-> (ListObjectsV2Response/builder)
            (.contents objects)
            (.isTruncated false)
            (.build))))))

(defn- gunzip-edn
  [bytes]
  (with-open [gzip (GZIPInputStream. (ByteArrayInputStream. bytes))]
    (edn/read-string (slurp gzip :encoding "UTF-8"))))

(defn- count-requests
  [requests op key]
  (count (filter #(and (= op (first %)) (= key (second %))) @requests)))

(deftest pack-keys-use-inverted-pack-indexes
  (is (= [0 999] (packs/pack-range 0)))
  (is (= [1000 1999] (packs/pack-range 1)))
  (is (= 0 (packs/commit-number->pack-index 999)))
  (is (= 1 (packs/commit-number->pack-index 1000)))
  (is (= "9223372036854775807" (packs/format-pack-index 0)))
  (is (= "9223372036854775806" (packs/format-pack-index 1)))
  (is (= 1 (packs/parse-pack-index (packs/format-pack-index 1))))
  (is (= "packs/9223372036854775807" (packs/pack-key "" 0)))
  (is (= "streams/acme/packs/9223372036854775806"
         (packs/pack-key "/streams/acme/" 1))))

(deftest try-pack-creates-gzipped-edn-pack
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client
              :bucket "events"
              :prefix prefix
              :headers {"X-Tigris-Consistent" "true"}}
        source-store (s3/store opts)]
    (seed-commits! objects packs/pack-size)
    (is (true? (packs/try-pack! opts source-store 0)))
    (is (false? (packs/try-pack! opts source-store 0)))
    (let [pack-object (get @objects (packs/pack-key prefix 0))
          commits (gunzip-edn pack-object)]
      (is (= packs/pack-size (count commits)))
      (is (= (commit 0) (first commits)))
      (is (= (commit 999) (last commits))))
    (is (= {"X-Tigris-Consistent" ["true"]
            "If-None-Match" ["*"]}
           (->> @requests
                (filter #(= [:put (packs/pack-key prefix 0)] (take 2 %)))
                first
                peek)))))

(deftest try-pack-refuses-to-create-pack-gaps
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client :bucket "events" :prefix prefix}
        source-store (s3/store opts)]
    (seed-commits! objects (* 2 packs/pack-size))
    (try
      (packs/try-pack! opts source-store 1)
      (is false "expected gap exception")
      (catch clojure.lang.ExceptionInfo e
        (is (= :gap (:error (ex-data e))))
        (is (= 1 (:pack-index (ex-data e))))
        (is (not (contains? (ex-data e) :expected)))))))

(deftest try-pack-throws-when-a-commit-is-missing
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client :bucket "events" :prefix prefix}
        source-store (s3/store opts)]
    (seed-commits! objects 999)
    (try
      (packs/try-pack! opts source-store 0)
      (is false "expected missing commit exception")
      (catch clojure.lang.ExceptionInfo e
        (is (= :missing-commit (:error (ex-data e))))
        (is (= 999 (:commit-number (ex-data e))))))))

(deftest pack-completed-ranges-packs-only-full-ranges
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client :bucket "events" :prefix prefix}
        source-store (s3/store opts)]
    (seed-commits! objects 2001)
    (is (= [true true] (packs/pack-completed-ranges! opts source-store)))
    (is (contains? @objects (packs/pack-key prefix 0)))
    (is (contains? @objects (packs/pack-key prefix 1)))
    (is (not (contains? @objects (packs/pack-key prefix 2))))
    (is (= 1 (count-requests requests :list (packs/packs-prefix prefix))))))

(deftest replay-store-uses-pack-and-falls-back-for-tail
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client :bucket "events" :prefix prefix}
        source-store (s3/store opts)]
    (seed-commits! objects 1001)
    (is (true? (packs/try-pack! opts source-store 0)))
    (reset! requests [])
    (let [store (packs/store opts)]
      (is (= (commit 0) (p/get-commit store 0)))
      (is (= (commit 999) (p/get-commit store 999)))
      (is (= (commit 1000) (p/get-commit store 1000)))
      (is (= 1000 (p/latest-commit-number store)))
      (is (zero? (count-requests requests :get (s3/commit-key prefix 0))))
      (is (= 1 (count-requests requests :list (packs/packs-prefix prefix))))
      (is (= 1 (count-requests requests :get (packs/pack-key prefix 0))))
      (is (zero? (count-requests requests :get (packs/pack-key prefix 1))))
      (is (= 1 (count-requests requests :get (s3/commit-key prefix 1000)))))))

(deftest replay-store-lists-packs-once-and-falls-back-when-none-exist
  (let [objects (atom {})
        requests (atom [])
        client (fake-s3-client objects requests)
        opts {:client client :bucket "events" :prefix prefix}
        store (packs/store opts)]
    (seed-commits! objects 2)
    (is (= (commit 0) (p/get-commit store 0)))
    (is (= (commit 1) (p/get-commit store 1)))
    (is (= 1 (count-requests requests :list (packs/packs-prefix prefix))))
    (is (zero? (count-requests requests :get (packs/pack-key prefix 0))))
    (is (= 1 (count-requests requests :get (s3/commit-key prefix 0))))
    (is (= 1 (count-requests requests :get (s3/commit-key prefix 1))))))

(deftest pack-aware-store-appends-through-s3-commits
  (let [objects (atom {})
        requests (atom [])
        store (packs/store {:client (fake-s3-client objects requests)
                            :bucket "events"
                            :prefix prefix})]
    (is (true? (p/try-append! store 0 (commit 0))))
    (is (false? (p/try-append! store 0 (commit 0))))
    (is (true? (p/try-append! store 1 (commit 1))))
    (is (= (commit 0) (p/get-commit store 0)))
    (is (= (commit 1) (p/get-commit store 1)))
    (is (contains? @objects (s3/commit-key prefix 0)))
    (is (contains? @objects (s3/commit-key prefix 1)))
    (is (= 1 (count-requests requests :head (s3/commit-key prefix 0))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.eventstore.s3-packs-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
