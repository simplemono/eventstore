(ns simplemono.eventstore.memory-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.eventstore.memory :as memory]
            [simplemono.eventstore.protocols :as p]))

(def commit-0
  {:commit/number 0
   :commit/id #uuid "00000000-0000-0000-0000-000000000001"
   :commit/timestamp #inst "2026-01-01T00:00:00.000-00:00"
   :commit/events [{:event/id #uuid "00000000-0000-0000-0000-000000000101"
                    :event/kind :example/created}]})

(def commit-1
  {:commit/number 1
   :commit/id #uuid "00000000-0000-0000-0000-000000000002"
   :commit/timestamp #inst "2026-01-01T00:00:01.000-00:00"
   :commit/events [{:event/id #uuid "00000000-0000-0000-0000-000000000102"
                    :event/kind :example/updated}]})

(deftest try-append-is-create-only-and-gap-free
  (let [store (memory/store)]
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
    (is (nil? (p/get-commit store 2)))))

(deftest ambiguous-put-fault-injection-writes-the-commit
  (let [seen? (atom false)
        store (memory/store {:after-put (fn [_n _commit]
                                          (when-not @seen?
                                            (reset! seen? true)
                                            true))})]
    (testing "the store can simulate a put that succeeded but returned timeout"
      (try
        (p/try-append! store 0 commit-0)
        (is false "expected ambiguous append exception")
        (catch clojure.lang.ExceptionInfo e
          (is (= :ambiguous (:error (ex-data e))))
          (is (= 0 (:commit-number (ex-data e))))
          (is (= (:commit/id commit-0) (:commit-id (ex-data e))))))
      (is (= commit-0 (p/get-commit store 0)))
      (is (= 0 (p/latest-commit-number store))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.eventstore.memory-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
