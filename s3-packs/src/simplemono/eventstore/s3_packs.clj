(ns simplemono.eventstore.s3-packs
  "Optional S3 pack optimization for cold replay.

   Individual commit objects remain the source of truth. Packs are immutable,
   create-only gzip objects containing one EDN vector of commits. Each pack
   covers exactly 1000 commits in ascending commit-number order.

   Pack objects live under the same stream prefix as commits:

     {prefix}/packs/{inverted-pack-index-019d}

   Pack indexes use the same inverted Long/MAX_VALUE key-space as S3 commit
   objects, so the newest pack sorts first in a normal lexicographic LIST.
   Examples:

     commits 0..999     -> packs/9223372036854775807
     commits 1000..1999 -> packs/9223372036854775806

   Pack creation is gap-free and create-only. try-pack! returns true when it
   creates the next expected pack, false when that pack already exists, and
   throws {:error :gap} if asked to skip a pack index.

   A pack-aware replay store implements EventStore reads by listing the packs/
   prefix once to find the latest pack index, assuming packs are contiguous from
   0 through that latest index, serving those full 1000-commit ranges from
   packs, and falling back to the underlying S3 EventStore for the unpacked
   tail. It keeps only the current pack in memory and does not prefetch. Missing
   packs inside the discovered packed range are treated as invariant violations
   and throw {:error :missing-pack}."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [simplemono.eventstore.protocols :as p]
            [simplemono.eventstore.s3 :as s3])
  (:import (java.io ByteArrayOutputStream)
           (java.nio.charset StandardCharsets)
           (java.util.function Consumer)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (software.amazon.awssdk.core.exception SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                      ListObjectsV2Request
                                                      ListObjectsV2Response
                                                      NoSuchKeyException
                                                      PutObjectRequest
                                                      S3Exception
                                                      S3Object)))

(def pack-size
  "Number of commits in a full pack."
  1000)

(defn commit-number->pack-index
  "Return the zero-based pack index that would contain commit-number."
  [commit-number]
  (quot (long commit-number) pack-size))

(defn pack-range
  "Return [from to] commit numbers covered by pack-index."
  [pack-index]
  (let [from (* (long pack-index) pack-size)]
    [from (+ from (dec pack-size))]))

(defn format-pack-index
  "Format pack-index as the 19-digit inverted key segment."
  [pack-index]
  (s3/format-inverted-commit-number pack-index))

(defn parse-pack-index
  "Parse a 19-digit inverted pack key segment to a normal pack index."
  [s]
  (s3/parse-inverted-commit-number s))

(defn- normalized-prefix
  [prefix]
  (str/replace (str (or prefix "")) #"^/+|/+$" ""))

(defn packs-prefix
  "Return the object prefix used for pack objects."
  [prefix]
  (let [prefix (normalized-prefix prefix)]
    (if (str/blank? prefix)
      "packs/"
      (str prefix "/packs/"))))

(defn pack-key
  "Return the object key for pack-index under prefix."
  [prefix pack-index]
  (str (packs-prefix prefix) (format-pack-index pack-index)))

(defn- key->pack-index
  [packs-prefix key]
  (when (str/starts-with? key packs-prefix)
    (let [segment (subs key (count packs-prefix))]
      (when (re-matches #"\d{19}" segment)
        (try
          (parse-pack-index segment)
          (catch NumberFormatException _
            nil))))))

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

(defn- put-pack-request
  ^PutObjectRequest
  [bucket key headers]
  (-> (PutObjectRequest/builder)
      (.bucket bucket)
      (.key key)
      (.overrideConfiguration (override headers true))
      (.contentType "application/edn; charset=utf-8")
      (.contentEncoding "gzip")
      (.build)))

(defn- get-pack-request
  ^GetObjectRequest
  [bucket key headers]
  (-> (GetObjectRequest/builder)
      (.bucket bucket)
      (.key key)
      (.overrideConfiguration (override headers false))
      (.build)))

(defn- list-packs-request
  ^ListObjectsV2Request
  [bucket prefix headers]
  (-> (ListObjectsV2Request/builder)
      (.bucket bucket)
      (.prefix prefix)
      (.maxKeys (int 1))
      (.overrideConfiguration (override headers false))
      (.build)))

(defn- latest-pack-index*
  [^S3Client client bucket prefix headers]
  (let [packs-prefix (packs-prefix prefix)
        request (list-packs-request bucket packs-prefix headers)
        ^ListObjectsV2Response response (.listObjectsV2 client request)]
    (when-let [object (first (.contents response))]
      (key->pack-index packs-prefix (.key ^S3Object object)))))

(defn- pack-bytes
  [commits]
  (let [out (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. out)]
      (.write gzip (.getBytes (pr-str (vec commits)) StandardCharsets/UTF_8)))
    (.toByteArray out)))

(defn- read-pack
  [in]
  (with-open [gzip (GZIPInputStream. in)]
    (vec (edn/read-string (slurp gzip :encoding "UTF-8")))))

(defn- missing-commit!
  [pack-index commit-number]
  (throw (ex-info "Cannot pack missing commit"
                  {:error :missing-commit
                   :pack-index pack-index
                   :commit-number commit-number})))

(defn- pack-gap!
  [pack-index]
  (throw (ex-info "Pack write would create a gap"
                  {:error :gap
                   :pack-index pack-index})))

(defn- missing-pack!
  [pack-index key cause]
  (throw (ex-info "Expected pack is missing"
                  {:error :missing-pack
                   :pack-index pack-index
                   :key key}
                  cause)))

(defn- ambiguous-pack!
  [pack-index cause]
  (throw (ex-info "Pack write outcome ambiguous"
                  {:error :ambiguous
                   :pack-index pack-index}
                  cause)))

(defn- invalid-pack!
  [pack-index commit-count]
  (throw (ex-info "Invalid pack object"
                  {:error :invalid-pack
                   :pack-index pack-index
                   :actual commit-count})))

(defn- s3-config
  [{:keys [client bucket prefix headers]}]
  (when-not client
    (throw (ex-info "S3 packs require :client" {})))
  (when (str/blank? (str bucket))
    (throw (ex-info "S3 packs require :bucket" {})))
  {:client client
   :bucket bucket
   :prefix (normalized-prefix prefix)
   :headers (or headers {})})

(defn- write-pack!
  [{:keys [^S3Client client bucket prefix headers]} source-store pack-index]
  (let [[from to] (pack-range pack-index)
        commits (mapv (fn [commit-number]
                        (or (p/get-commit source-store commit-number)
                            (missing-commit! pack-index commit-number)))
                      (range from (inc to)))
        key (pack-key prefix pack-index)
        request (put-pack-request bucket key headers)]
    (try
      (.putObject client request (RequestBody/fromBytes (pack-bytes commits)))
      true
      (catch S3Exception e
        (if (service-conflict? e)
          false
          (throw e)))
      (catch SdkClientException e
        (ambiguous-pack! pack-index e)))))

(defn try-pack!
  "Create the full pack for pack-index from source-store.

   Reads exactly 1000 individual commits from source-store. Returns true when
   the pack object was created and false when the pack already exists or another
   writer won the create-only race. Throws if pack-index would create a gap or
   any commit in the range is missing."
  [opts source-store pack-index]
  (let [{:keys [^S3Client client bucket prefix headers] :as config} (s3-config opts)
        pack-index (long pack-index)
        expected (if-some [latest (latest-pack-index* client bucket prefix headers)]
                   (inc latest)
                   0)]
    (cond
      (< pack-index expected)
      false

      (> pack-index expected)
      (pack-gap! pack-index)

      :else
      (write-pack! config source-store pack-index))))

(defn pack-completed-ranges!
  "Try to create all full packs up to source-store's current head.

   Lists the packs prefix once to find the newest pack, relies on the pack
   sequence being gap-free, and then writes only later full ranges. Returns a
   vector of booleans, one per attempted pack index. true means the pack was
   created; false means it already existed or another writer won."
  [opts source-store]
  (if-some [latest (p/latest-commit-number source-store)]
    (let [{:keys [^S3Client client bucket prefix headers] :as config} (s3-config opts)
          full-pack-count (quot (inc (long latest)) pack-size)
          start (if-some [latest-pack (latest-pack-index* client bucket prefix headers)]
                  (inc latest-pack)
                  0)]
      (mapv #(write-pack! config source-store %)
            (range start full-pack-count)))
    []))

(defn- load-pack
  [{:keys [^S3Client client bucket prefix headers]} pack-index]
  (let [key (pack-key prefix pack-index)
        request (get-pack-request bucket key headers)]
    (try
      (with-open [in (.getObject client request)]
        (let [commits (read-pack in)]
          (when-not (= pack-size (count commits))
            (invalid-pack! pack-index (count commits)))
          commits))
      (catch NoSuchKeyException e
        (missing-pack! pack-index key e))
      (catch S3Exception e
        (if (service-not-found? e)
          (missing-pack! pack-index key e)
          (throw e))))))

(defn- latest-pack-index
  [{:keys [state] :as store}]
  (let [state-value @state]
    (if (contains? state-value :latest-pack-index)
      (:latest-pack-index state-value)
      (let [{:keys [^S3Client client bucket prefix headers]} store
            latest (latest-pack-index* client bucket prefix headers)]
        (swap! state assoc :latest-pack-index latest)
        latest))))

(defn- packed-range?
  [store pack-index]
  (if-some [latest (latest-pack-index store)]
    (<= pack-index latest)
    false))

(defn- get-pack
  [{:keys [state] :as store} pack-index]
  (or (when (= pack-index (get-in @state [:current-pack :index]))
        (get-in @state [:current-pack :commits]))
      (let [commits (load-pack store pack-index)]
        (swap! state assoc :current-pack {:index pack-index
                                          :commits commits})
        commits)))

(defrecord PackedReplayEventStore [^S3Client client bucket prefix headers delegate
                                   state]
  p/EventStore
  (try-append! [_ commit-number _commit]
    (throw (ex-info "Pack-aware replay store is read-only"
                    {:error :read-only
                     :commit-number (long commit-number)})))

  (get-commit [this commit-number]
    (let [commit-number (long commit-number)
          pack-index (commit-number->pack-index commit-number)
          offset (mod commit-number pack-size)]
      (if (packed-range? this pack-index)
        (nth (get-pack this pack-index) offset nil)
        (p/get-commit delegate commit-number))))

  (latest-commit-number [_]
    (p/latest-commit-number delegate)))

(defn store
  "Create a read-only, pack-aware replay EventStore.

   Options are the same as simplemono.eventstore.s3/store. Reads list the packs
   prefix once to find the latest contiguous pack index. Commit numbers covered
   by that packed range are read from deterministic pack keys; later tail
   commits are read through the underlying S3 store. try-append! always throws
   {:error :read-only}."
  [opts]
  (let [{:keys [client bucket prefix headers]} (s3-config opts)
        delegate (s3/store {:client client
                            :bucket bucket
                            :prefix prefix
                            :headers headers})]
    (->PackedReplayEventStore client bucket prefix headers delegate
                              (atom {:current-pack nil}))))
