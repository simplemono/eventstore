# Event Store for Clojure — Minimal Design Spec

A minimal, production-oriented append-only event log, backed by S3-compatible
object storage. Each `EventStore` instance is scoped to one ordered stream
prefix.

The event store does **not** know about tenants, commands, projections,
idempotency, SQLite, retries, or read models. Those are application concerns.
A multi-tenant SaaS can route each tenant to its own event-store prefix; a
webhook ingester can route each provider/account to another prefix.

## Motivation

- **Bottomless storage**: the event log only grows; object storage removes the
  "dedicated server disk fills up" failure mode of Postgres/SQLite-as-log.
- **Simple core**: the library owns only immutable commit storage and point
  reads. Application code owns command handling and derived state.
- **Composable streams**: tenants, webhook inboxes, audit logs, and integration
  streams are all just prefixes chosen at the application edge.

## Correctness invariants

- Commit keys are zero-based and gap-free within a stream: commit 0 is first;
  commit N+1 cannot exist unless N exists.
- Commit objects are immutable.
- `try-append!` is create-only. It returns `true` when appended and `false`
  when the key already exists.
- A commit may contain multiple events; appending that commit is atomic from the
  event store's perspective.
- Packs are optimizations only; deleting them never loses source data.

## Storage layout (S3-compatible object storage)

An `EventStore` instance is constructed with one storage prefix. Under that
prefix it uses:

```text
{prefix}/commits/{inverted-commit-number-019d} ; one object per commit, immutable
{prefix}/packs/{inverted-pack-index-019d}      ; optional 1000-commit packs
```

The application edge maps domain identities to prefixes, e.g.
`streams/tenant/acme`, `webhooks/stripe`, or
`tenants/{tenant-id}/eventstore`. The core protocols never take a `stream-id`.

- **Commit numbers** are zero-based non-negative Longs. The S3 adapter stores
  the inverted value `Long/MAX_VALUE - commit-number`, zero-padded to **19
  digits** (Long/MAX_VALUE = 9223372036854775807 has 19 digits). Commit 0 is
  `9223372036854775807`, commit 1 is `9223372036854775806`, and newer commits
  sort before older commits in lexicographic LIST results.
- **Commit objects are immutable and never deleted by the event store**. Packs
  are a pure read/cost optimization, never a source of truth.
- **Packs cover fixed ranges** (`…000000-…000999`). Implementations can compute
  pack keys for point reads and fall back to individual commit objects when a
  pack is missing.
- The S3 store writes individual commit objects as gzip-compressed EDN
  (`Content-Encoding: gzip`). A gzipped pack cannot be range-read; reading any
  commit from a pack means downloading the whole pack. Acceptable at
  1000-commit granularity.

## Object-store requirements

The design assumes the backing object store provides these semantics for the
configured prefix:

- **Atomic create-only put**: conditional put / `If-None-Match: *` succeeds for
  exactly one writer for a given commit key and fails for all contenders. The S3
  adapter accepts extra request headers for provider-specific consistency
  controls, e.g. `X-Tigris-Consistent: true`.
- **Strong read-after-write for GET/HEAD**: once a commit put succeeds, a GET of
  that key returns the object.
- **Ambiguous timeout disambiguation**: after a put timeout, GETting the
  disputed commit key returns the winning object if any. Comparing `:commit/id`
  tells whether our write succeeded or a competitor won.
- **Scalar head lookup**: `latest-commit-number` returns a commit number, not a lazy
  resource. The S3 implementation computes it with LIST because inverted commit
  keys make the newest commit sort first. Other implementations may use a native
  latest-object feature, cached state, or N+1 probing.

Any object-store backend must satisfy these requirements or provide an adapter
that restores them.

## Commit data

`try-append!` receives a commit as Clojure data. Implementations choose their
own physical encoding. The local file store uses plain EDN via `pr-str` /
`clojure.edn/read-string`; the S3 store uses gzip-compressed EDN with
`Content-Encoding: gzip`. A production implementation may choose a different
encoding such as Nippy bytes.

```clojure
{:commit/number     42
 :commit/id         #uuid "..."
 :commit/timestamp  #inst "..."
 :commit/events     [{:event/id #uuid "..." :event/type :order/placed ...}
                     {:event/id #uuid "..." :event/type :payment/captured ...}]}
```

- `:commit/id` disambiguates ambiguous append timeouts: after a timeout, GET the
  disputed commit key and compare ids.
- `:event/id` is a UUID per event.
- Commits are never rewritten.

## API sketch

```clojure
(ns simplemono.eventstore.protocols)

(defprotocol EventStore
  (try-append! [store commit-number commit]
    "Create-only put at zero-based commit-number in this store's stream.
     Returns true when appended, false when commit-number already exists.
     Throws ex-info for exceptional states such as :gap or :ambiguous.")

  (get-commit [store commit-number]
    "Return the commit at commit-number, or nil when absent.")

  (latest-commit-number [store]
    "Return the latest commit number, or nil for an empty stream."))
```

Modules:

- `protocols/` — `simplemono.eventstore.protocols`.
- `s3/` — `simplemono.eventstore.s3`, constructed with a bucket and prefix.
  Uses inverted commit keys for efficient head lookup. Depends on the AWS SDK.
- `file/` — `simplemono.eventstore.file`, local development EventStore,
  constructed with a dir. Does not depend on the AWS SDK.
- `memory/` — `simplemono.eventstore.memory`, pure in-memory EventStore for
  tests.
- `s3-packs/` — `simplemono.eventstore.s3-packs`, optional S3 cold-replay
  optimization. It creates immutable pack objects and provides a read-only
  pack-aware replay `EventStore`.

## Application-owned command loop

The application decides whether events should be committed. A typical server-side
command loop is:

1. Catch application projections up from the event store.
2. Validate the command against those projections.
3. Derive one or more event maps.
4. Build a commit with a fresh `:commit/id`.
5. Attempt `try-append!` at the expected next commit number (`0` for an empty
   stream, otherwise `head + 1`).
6. If it returns `false`, catch projections up, re-run command handling from
   scratch, and retry or return a conflict to the caller.
7. On ambiguous timeout exception: `get-commit` the disputed number and compare
   `:commit/id`. If it is ours, success; otherwise handle as conflict.

The event store intentionally does not implement this loop. Different apps may
choose server-owned retries, client-owned retries, explicit `409 Conflict`, one
projection, many projections, or different idempotency policies.

## Read/catch-up pattern

Application projections should catch up with scalar head + point reads:

1. Read `head = (latest-commit-number store)`.
2. If `head` is nil or `<= local-checkpoint`, the projection is fresh as of that
   scalar lookup.
3. For each commit number from `local-checkpoint + 1` through `head`, call
   `get-commit` and apply the commit. A projection that has applied nothing can
   use `-1` as its local checkpoint.
4. If a commit before the reported head is missing, treat it as a store
   invariant violation.

The public API does not expose a lazy commit sequence, so callers cannot
accidentally consume a sequence after the store has closed an underlying
resource.

## Schema evolution and projections

Projection storage, checkpointing, schema versions, upcasters, rebuilds, and
read models are application concerns. A SQLite projection might use
`PRAGMA user_version` for its schema/projection version and a checkpoint table
for the last applied commit, but that is outside the event-store core.

## Packing

For S3-compatible stores, the optional `s3-packs/` module can build cold-replay
packs from immutable individual commit objects. Individual commits remain the
source of truth; packs are only a request-count and replay-throughput
optimization.

Packs cover full closed ranges of 1000 commits:

```text
{prefix}/packs/{inverted-pack-index-019d}

commits 0..999     -> {prefix}/packs/9223372036854775807
commits 1000..1999 -> {prefix}/packs/9223372036854775806
```

Pack indexes use the same inverted-number key-space as commits, so the newest
pack sorts first under `packs/`. Pack objects are gzip-compressed EDN vectors
serialized with `pr-str`, in ascending commit-number order. Only full ranges are
eligible. A packer must fail if any commit in the range is missing.

Pack creation is external/explicit. `try-pack!` creates one deterministic pack
with conditional put / `If-None-Match: *`; racing packers are safe because one
wins and the others return false. Pack creation is gap-free: attempting to write
beyond the next expected pack throws `{:error :gap}`. `pack-completed-ranges!`
attempts all full ranges below the current head.

The pack-aware replay store is read-only. It implements the same `EventStore`
read protocol: `latest-commit-number` delegates to the S3 store. On first
packed read, `get-commit` lists `packs/` once to find the newest pack and then
assumes packs are contiguous from pack 0 through that newest pack. Commit
numbers covered by that packed range are loaded from deterministic pack keys;
the unpacked tail falls back to individual commit objects. During sequential
replay it keeps one current pack in memory.

Missing packs inside the discovered packed range are treated as store invariant
violations. Deleting packs can therefore break a pack-aware replay store;
rebuild packs or use the plain S3 store to replay without packs.

## Testing

- The in-memory `EventStore` implementation makes app command loops testable
  without network.
- Store contract tests should verify create-only append, gap detection,
  conflict detection, point reads, scalar head lookup, and ambiguous timeout
  disambiguation.
- App-level/property tests should verify that concurrent command handlers produce
  a gap-free, duplicate-free log and convergent projections.

## Open items

- Optional provider-specific optimizations for scalar head lookup.
- Optional hash-chain metadata.
