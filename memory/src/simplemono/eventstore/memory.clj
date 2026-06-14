(ns simplemono.eventstore.memory
  (:require [simplemono.eventstore.protocols :as p]))

(defn- gap! [commit-number expected]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :commit-number commit-number
                   :expected expected})))

(defn- ambiguous! [commit-number commit]
  (throw (ex-info "Append outcome ambiguous"
                  {:error :ambiguous
                   :commit-number commit-number
                   :commit-id (:commit/id commit)})))

(defrecord MemoryEventStore [state after-put]
  p/EventStore
  (try-append! [_ commit-number commit]
    (locking state
      (let [commit-number (long commit-number)
            stream @state
            next-number (if-some [latest (last (keys stream))]
                          (inc latest)
                          0)]
        (cond
          (contains? stream commit-number)
          false

          (not= commit-number next-number)
          (gap! commit-number next-number)

          :else
          (do
            (swap! state assoc commit-number commit)
            (if (and after-put (after-put commit-number commit))
              (ambiguous! commit-number commit)
              true))))))

  (get-commit [_ commit-number]
    (get @state (long commit-number)))

  (latest-commit-number [_]
    (last (keys @state))))

(defn store
  "Create an in-memory EventStore scoped to one stream.

   Options:
   - :after-put fn of commit-number, commit -> truthy. When truthy, the commit
     is written but try-append! throws an :ambiguous ex-info to exercise timeout
     disambiguation paths."
  ([] (store {}))
  ([{:keys [after-put]}]
   (->MemoryEventStore (atom (sorted-map)) after-put)))
