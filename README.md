# Event Store library for Clojure

A small Clojure event-store library based on the design in
`event-store-design.md`.

## Why

I want to use it for our projects. We are running a SaaS with a cell-based
architecture. More tenants on a cell (a dedicated server) means less disk space
left. Events are immutable, so disk usage will always increase. Events will only
be deleted, when the tenant (customer) is being deleted.

The eventstore library allows you to store the events (source of truth) on a
S3-compatible object-store. To avoid that your write-latency is becoming a
nightmare you should better have one event-stream per tenant and use an
object-store like Tigrisdata, Amazon S3 Express One Zone or Google Cloud Storage
Rapid Bucket (which all offer a relatively low write latency).

Queries will be done on your read-model that could for example be a Sqlite db on
the local disk. The events are the essential state while the read-model(s) are
derived state. For a schema migration, disaster recovery or when you move a
tenant from on cell to another, you just replay the events to recreate your
read-model (Sqlite db file). Therefore you do not need to setup Litestream or
any other backup mechanism for the read-model (you still can store snapshots if
you want to reduce the time-to-recovery).

## Introduction

The library is intentionally minimal: it provides an append-only, single-stream
commit log. Command handling, projections, retries, and idempotency live in the
application.

New low-latency object stores make this design attractive: Tigris Object
Storage, Amazon S3 Express One Zone, and Google Cloud Storage Rapid Bucket are
examples of object-store products aimed at lower-latency writes and reads than
traditional object-storage usage patterns.

Use one `EventStore` per customer/tenant, with a distinct bucket prefix for that
customer/tenant. Do not use one shared `EventStore` instance for all customers.

Modules live in separate top-level directories, each with its own `deps.edn`.
There is intentionally no root `deps.edn`.

- `protocols/` — `simplemono.eventstore.protocols`, the single-stream
  `EventStore` protocol with `try-append!`, `get-commit`, and scalar
  `latest-commit-number`.
- `memory/` — `simplemono.eventstore.memory`, an in-memory single-stream
  `EventStore` for tests and simulations. Depends on `protocols/`.
- `file/` — `simplemono.eventstore.file`, a local-file single-stream
  `EventStore` using the object-store-like layout
  `root/commits/{commit-number-019d}` with create-only writes. Depends on
  `protocols/`, but not on the AWS SDK.
- `s3/` — `simplemono.eventstore.s3`, a generic S3-compatible single-stream
  `EventStore` using conditional create-only puts and inverted commit-number
  keys so LIST returns the newest commit first. Depends on `protocols/` and the
  AWS SDK.
- `s3-packs/` — `simplemono.eventstore.s3-packs`, an optional cold-replay
  optimization for S3 stores. It can create immutable 1000-commit gzip packs and
  provide a read-only pack-aware replay `EventStore`. Depends on `protocols/`
  and `s3/`.

S3 stores are constructed with a bucket and stream prefix, e.g.
`streams/tenant/acme`, and then implement only single-stream commit operations.

## Run tests

```sh
(cd file && clojure -M:test)
(cd memory && clojure -M:test)
(cd s3 && clojure -M:test)
(cd s3-packs && clojure -M:test)
```

## Dependency examples

Use only the implementation module you need:

```clojure
;; File store, no AWS SDK dependency:
simplemono/eventstore-file {:local/root "file"}

;; S3 store, includes the AWS SDK:
simplemono/eventstore-s3 {:local/root "s3"}

;; Optional pack-aware replay store and pack writer:
simplemono/eventstore-s3-packs {:local/root "s3-packs"}
```

## Tigris Data usage

Use the generic S3 adapter with Tigris's consistency header:

```clojure
(require '[simplemono.eventstore.s3 :as s3])

(def client
  (s3/client {:endpoint "https://t3.storage.dev"
              :region "auto"
              :access-key-id "tid_..."
              :secret-access-key "tsec_..."}))

(def store
  (s3/store {:client client
             :bucket "events"
             :prefix "streams/tenant/acme"
             :headers {"X-Tigris-Consistent" "true"}}))
```

## Generic S3-compatible usage

```clojure
(require '[simplemono.eventstore.s3 :as s3])

(def client
  (s3/client {:endpoint "http://localhost:9000"
              :region "us-east-1"
              :access-key-id "minioadmin"
              :secret-access-key "minioadmin"
              :path-style? true}))

(def store
  (s3/store {:client client
             :bucket "events"
             :prefix "streams/tenant/acme"}))
```

Commit `0` is stored as
`streams/tenant/acme/commits/9223372036854775807`, commit `1` as
`streams/tenant/acme/commits/9223372036854775806`, and so on. This makes the
latest commit sort first for object stores that list keys lexicographically.
Commit objects are gzip-compressed EDN with `Content-Encoding: gzip`.

## Optional S3 packs

The `s3-packs/` module is for cold replay throughput and object-store request
cost. Individual gzip-compressed commit objects remain the source of truth;
packs are only an optimization.

Packs are full closed ranges of 1000 commits, stored under the same stream
prefix:

```text
{prefix}/packs/{inverted-pack-index-019d}
```

Examples:

```text
commits 0..999     -> {prefix}/packs/9223372036854775807
commits 1000..1999 -> {prefix}/packs/9223372036854775806
```

Each pack is a gzip object containing one EDN vector of commits serialized with
`pr-str`, in ascending commit-number order. Pack indexes use the same
inverted-number trick as commits, so the newest pack sorts first under `packs/`.
Pack writes are create-only and gap-free, so racing packers are safe: one wins
and the others return false.

```clojure
(require '[simplemono.eventstore.s3 :as s3]
         '[simplemono.eventstore.s3-packs :as packs])

(def source-store
  (s3/store {:client client
             :bucket "events"
             :prefix "streams/tenant/acme"}))

;; External/background job:
(packs/try-pack! {:client client
                  :bucket "events"
                  :prefix "streams/tenant/acme"}
                 source-store
                 0)
;; => true if created, false if already present

;; Replay process:
(def replay-store
  (packs/store {:client client
                :bucket "events"
                :prefix "streams/tenant/acme"}))
```

The pack-aware replay store implements the `EventStore` protocol for reads:
`latest-commit-number` delegates to the S3 store. On first packed read,
`get-commit` lists `packs/` once to find the newest pack and assumes packs are
contiguous from pack 0 through that pack. Packed ranges are served from packs;
the unpacked tail falls back to individual commit objects. `try-append!` throws
`{:error :read-only}`.

## Minimal usage

```clojure
(require '[simplemono.eventstore.memory :as memory]
         '[simplemono.eventstore.protocols :as p])

(def store (memory/store))

(def commit
  {:commit/number 0
   :commit/id (java.util.UUID/randomUUID)
   :commit/timestamp (java.util.Date.)
   :commit/events [{:event/id (java.util.UUID/randomUUID)
                    :event/type :example/happened}]})

(p/try-append! store 0 commit)
;; => true

(p/latest-commit-number store)
;; => 0

(p/get-commit store 0)
;; => commit
```

Application code owns the command loop, for example:

```clojure
;; catch projections up from the EventStore
;; validate a command against projections
;; derive event data
;; try-append! expected next commit number
;; false means conflict: catch up and re-run command handling
;; on ambiguous timeout exception, get-commit the disputed number and compare :commit/id
```

## License

MIT. See [LICENSE](LICENSE).
