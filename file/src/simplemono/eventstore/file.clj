(ns simplemono.eventstore.file
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [simplemono.eventstore.protocols :as p])
  (:import (java.nio.charset StandardCharsets)
           (java.nio.file Files Path StandardOpenOption FileAlreadyExistsException)))

(defn- root-path [root]
  (.toPath (io/file root)))

(defn- commits-dir [root]
  (.resolve (root-path root) "commits"))

(defn- format-commit-number
  "Format a non-negative Long so lexicographic order matches numeric order."
  [n]
  (format "%019d" (long n)))

(defn- parse-commit-number
  [s]
  (Long/parseLong s))

(defn- commit-path [root commit-number]
  (.resolve (commits-dir root) (format-commit-number commit-number)))

(defn- ensure-commits-dir! [root]
  (Files/createDirectories (commits-dir root) (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- read-commit-path [^Path path]
  (when (Files/exists path (make-array java.nio.file.LinkOption 0))
    (edn/read-string (Files/readString path))))

(defn- last-commit-number-in-dir [root]
  (let [dir (commits-dir root)]
    (when (Files/isDirectory dir (make-array java.nio.file.LinkOption 0))
      (with-open [stream (Files/list dir)]
        (when-let [key (->> (iterator-seq (.iterator stream))
                            (map #(.getFileName ^Path %))
                            (map str)
                            (filter #(re-matches #"\d{19}" %))
                            sort
                            last)]
          (parse-commit-number key))))))

(defn- gap! [commit-number expected]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :commit-number commit-number
                   :expected expected})))

(defrecord FileEventStore [root]
  p/EventStore
  (try-append! [_ commit-number commit]
    (let [commit-number (long commit-number)
          path (commit-path root commit-number)]
      (ensure-commits-dir! root)
      (cond
        (Files/exists path (make-array java.nio.file.LinkOption 0))
        false

        :else
        (let [expected (if-some [latest (last-commit-number-in-dir root)]
                         (inc latest)
                         0)]
          (when-not (= commit-number expected)
            (gap! commit-number expected))
          (try
            (Files/writeString path
                               (pr-str commit)
                               StandardCharsets/UTF_8
                               (into-array StandardOpenOption
                                           [StandardOpenOption/CREATE_NEW
                                            StandardOpenOption/WRITE]))
            true
            (catch FileAlreadyExistsException _
              false))))))

  (get-commit [_ commit-number]
    (read-commit-path (commit-path root (long commit-number))))

  (latest-commit-number [_]
    (last-commit-number-in-dir root)))

(defn store
  "Create a local-file EventStore scoped to one stream prefix.

   Layout under root:
     commits/{commit-number-019d}
     packs/{from}-{to}          ; future optimization

   It is useful for development and tests; it is not a substitute for
   production S3-compatible object storage because filesystem semantics differ
   from object-store semantics."
  [root]
  (->FileEventStore (str root)))
