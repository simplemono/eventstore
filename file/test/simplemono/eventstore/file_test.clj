(ns simplemono.eventstore.file-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [simplemono.eventstore.file :as file]
            [simplemono.eventstore.protocols :as p]))

(defn temp-dir []
  (str (java.nio.file.Files/createTempDirectory "eventstore-file-test"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(def commit-0
  {:commit/number 0
   :commit/id #uuid "00000000-0000-0000-0000-000000000001"
   :commit/timestamp #inst "2026-01-01T00:00:00.000-00:00"
   :commit/events [{:event/id #uuid "00000000-0000-0000-0000-000000000101"
                    :event/type :example/created}]})

(deftest file-store-uses-create-only-commits
  (let [store (file/store (temp-dir))]
    (is (nil? (p/latest-commit-number store)))
    (is (true? (p/try-append! store 0 commit-0)))
    (is (false? (p/try-append! store 0 commit-0)))
    (is (= 0 (p/latest-commit-number store)))
    (is (= commit-0 (p/get-commit store 0)))
    (is (nil? (p/get-commit store 1)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.eventstore.file-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
