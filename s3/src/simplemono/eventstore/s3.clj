(ns simplemono.eventstore.s3
  "S3-compatible EventStore implementation.

   The public API uses normal zero-based commit numbers. Object keys use an
   inverted Long/MAX_VALUE key-space so the newest commit sorts first under the
   commits prefix when the object store lists keys lexicographically ascending.
   Commit objects are gzip-compressed EDN."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [simplemono.eventstore.protocols :as p])
  (:import (java.io ByteArrayOutputStream)
           (java.net URI)
           (java.nio.charset StandardCharsets)
           (java.util.function Consumer)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                     DefaultCredentialsProvider
                                                     StaticCredentialsProvider)
           (software.amazon.awssdk.core.exception SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                      HeadObjectRequest
                                                      ListObjectsV2Request
                                                      ListObjectsV2Response
                                                      NoSuchKeyException
                                                      PutObjectRequest
                                                      S3Exception
                                                      S3Object)))

(def commit-number-width
  "Width needed to zero-pad any non-negative signed 64-bit Long."
  19)

(defn invert-commit-number
  "Map a zero-based commit number into descending Long key-space.

   0 -> 9223372036854775807
   1 -> 9223372036854775806
   2 -> 9223372036854775805"
  [commit-number]
  (- Long/MAX_VALUE (long commit-number)))

(defn uninvert-commit-number
  "Map an inverted object-name number back to a normal zero-based commit number."
  [inverted]
  (- Long/MAX_VALUE (long inverted)))

(defn format-inverted-commit-number
  "Format commit-number as the 19-digit inverted object-name segment."
  [commit-number]
  (format (str "%0" commit-number-width "d")
          (invert-commit-number commit-number)))

(defn parse-inverted-commit-number
  "Parse a 19-digit inverted object-name segment to a normal commit number."
  [s]
  (uninvert-commit-number (Long/parseLong s)))

(defn- normalized-prefix
  [prefix]
  (str/replace (str (or prefix "")) #"^/+|/+$" ""))

(defn commits-prefix
  "Return the object prefix used for commit objects."
  [prefix]
  (let [prefix (normalized-prefix prefix)]
    (if (str/blank? prefix)
      "commits/"
      (str prefix "/commits/"))))

(defn commit-key
  "Return the object key for commit-number under prefix."
  [prefix commit-number]
  (str (commits-prefix prefix) (format-inverted-commit-number commit-number)))

(defn- key->commit-number
  [commits-prefix key]
  (when (str/starts-with? key commits-prefix)
    (let [segment (subs key (count commits-prefix))]
      (when (re-matches #"\d{19}" segment)
        (try
          (parse-inverted-commit-number segment)
          (catch NumberFormatException _
            nil))))))

(defn- gap!
  [commit-number]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :commit-number commit-number})))

(defn- ambiguous!
  [commit-number commit cause]
  (throw (ex-info "Append outcome ambiguous"
                  {:error :ambiguous
                   :commit-number commit-number
                   :commit-id (:commit/id commit)}
                  cause)))

(defn- service-conflict?
  [^S3Exception e]
  (contains? #{409 412} (.statusCode e)))

(defn- service-not-found?
  [^S3Exception e]
  (= 404 (.statusCode e)))

(defn- override
  [headers create-only?]
  (reify Consumer
    (accept [_ builder]
      (doseq [[k v] headers]
        (.putHeader builder (str k) (str v)))
      (when create-only?
        (.putHeader builder "If-None-Match" "*")))))

(defn- put-object-request
  ^PutObjectRequest
  [bucket key headers]
  (-> (PutObjectRequest/builder)
      (.bucket bucket)
      (.key key)
      (.overrideConfiguration (override headers true))
      (.contentType "application/edn; charset=utf-8")
      (.contentEncoding "gzip")
      (.build)))

(defn- gzip-bytes
  [s]
  (let [out (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. out)]
      (.write gzip (.getBytes (str s) StandardCharsets/UTF_8)))
    (.toByteArray out)))

(defn- get-object-request
  ^GetObjectRequest
  [bucket key headers]
  (-> (GetObjectRequest/builder)
      (.bucket bucket)
      (.key key)
      (.overrideConfiguration (override headers false))
      (.build)))

(defn- head-object-request
  ^HeadObjectRequest
  [bucket key headers]
  (-> (HeadObjectRequest/builder)
      (.bucket bucket)
      (.key key)
      (.overrideConfiguration (override headers false))
      (.build)))

(defn- list-objects-request
  ^ListObjectsV2Request
  [bucket prefix headers]
  (-> (ListObjectsV2Request/builder)
      (.bucket bucket)
      (.prefix prefix)
      (.maxKeys (int 1))
      (.overrideConfiguration (override headers false))
      (.build)))

(defn- latest-commit-number*
  [^S3Client client bucket prefix headers]
  (let [commits-prefix (commits-prefix prefix)
        request (list-objects-request bucket commits-prefix headers)
        ^ListObjectsV2Response response (.listObjectsV2 client request)]
    (when-let [object (first (.contents response))]
      (key->commit-number commits-prefix (.key ^S3Object object)))))

(defn- object-exists?
  [^S3Client client bucket key headers]
  (try
    (.headObject client (head-object-request bucket key headers))
    true
    (catch NoSuchKeyException _
      false)
    (catch S3Exception e
      (if (service-not-found? e)
        false
        (throw e)))))

(defrecord S3EventStore [^S3Client client bucket prefix headers]
  p/EventStore
  (try-append! [_ commit-number commit]
    (let [^S3Client client client
          commit-number (long commit-number)]
      ;; Avoid latest-commit-number* here: LIST is a Class A operation on object
      ;; stores such as Tigris, while HEAD is Class B (roughly 10x cheaper).
      ;; Checking the previous commit preserves gap-free append without listing
      ;; on every write.
      (if (or (zero? commit-number)
              (object-exists? client bucket (commit-key prefix (dec commit-number)) headers))
        (let [key (commit-key prefix commit-number)]
          (try
            (let [request (put-object-request bucket key headers)]
              (.putObject client
                          request
                          (RequestBody/fromBytes (gzip-bytes (pr-str commit)))))
            true
            (catch S3Exception e
              (if (service-conflict? e)
                false
                (throw e)))
            (catch SdkClientException e
              (ambiguous! commit-number commit e))))
        (gap! commit-number))))

  (get-commit [_ commit-number]
    (let [^S3Client client client
          key (commit-key prefix commit-number)]
      (try
        (let [request (get-object-request bucket key headers)]
          (with-open [in (.getObject client request)
                      gzip (GZIPInputStream. in)]
            (edn/read-string (slurp gzip :encoding "UTF-8"))))
        (catch NoSuchKeyException _
          nil)
        (catch S3Exception e
          (if (service-not-found? e)
            nil
            (throw e))))))

  (latest-commit-number [_]
    (let [^S3Client client client]
      (latest-commit-number* client bucket prefix headers))))

(defn client
  "Create an AWS SDK S3 client suitable for S3-compatible object stores.

   Options:
   - :endpoint              endpoint override string/URI, e.g. http://localhost:9000
   - :region                region string, default us-east-1
   - :access-key-id         static access key; omit to use default credentials
   - :secret-access-key     static secret key
   - :path-style?           true for path-style URLs, often needed for MinIO"
  [{:keys [endpoint region access-key-id secret-access-key path-style?]
    :or {region "us-east-1"}}]
  (let [builder (S3Client/builder)]
    (.region builder (Region/of region))
    (if access-key-id
      (.credentialsProvider builder
                            (StaticCredentialsProvider/create
                             (AwsBasicCredentials/create access-key-id
                                                         secret-access-key)))
      (.credentialsProvider builder (DefaultCredentialsProvider/create)))
    (when endpoint
      (.endpointOverride builder (if (instance? URI endpoint)
                                   endpoint
                                   (URI/create (str endpoint)))))
    (when (some? path-style?)
      (.serviceConfiguration builder
                             (reify Consumer
                               (accept [_ s3-config-builder]
                                 (.pathStyleAccessEnabled s3-config-builder
                                                          (boolean path-style?))))))
    (.build builder)))

(defn store
  "Create an S3-compatible EventStore scoped to one stream prefix.

   Required options:
   - :client  an S3Client
   - :bucket  bucket name
   - :prefix  stream prefix, e.g. streams/tenant/acme

   Optional options:
   - :headers map of extra request headers for provider-specific behavior

   Commit objects are gzip-compressed EDN and stored at:
     {prefix}/commits/{inverted-commit-number-019d}"
  [{:keys [client bucket prefix headers]}]
  (when-not client
    (throw (ex-info "S3 store requires :client" {})))
  (when (str/blank? (str bucket))
    (throw (ex-info "S3 store requires :bucket" {})))
  (->S3EventStore client bucket (normalized-prefix prefix) (or headers {})))
