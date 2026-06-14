(ns simplemono.eventstore.protocols)

(defprotocol EventStore
  (try-append! [store commit-number commit]
    "Create-only append at zero-based commit-number in this stream.

     The first commit in an empty stream is commit-number 0.

     Returns true when the commit was appended.
     Returns false when commit-number already exists.

     Throws ex-info for exceptional storage/caller states, with
     :error in ex-data. Expected values include:
       :gap       appending commit-number would create a gap
       :ambiguous append may have succeeded but the caller did not receive a
                  definitive result")
  (get-commit [store commit-number]
    "Return the commit at commit-number in this stream, or nil when absent.")
  (latest-commit-number [store]
    "Return the latest commit number, or nil for an empty stream."))
