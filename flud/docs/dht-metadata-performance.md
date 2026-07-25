# DHT Metadata Performance: Findings & Recommendations

## Purpose

flud uses a Kademlia-style DHT to store metadata about the files/objects it
backs up across the network. DHTs are notoriously non-performant at scale:
every operation costs multiple network round trips, and naive designs have
no good answer for frequently-mutated keys or partial replica failure. This
document lays out what flud's DHT layer actually does today, where that will
hurt as the network grows, and a staged set of alternatives — grounded in
recent (2024-2025) DHT and decentralized-storage research — that preserve
flud's decentralized, trust-minimized character while being measurably
faster. It closes with an explicit discussion of the consistency and
atomicity tradeoffs each recommendation makes, since "faster" and "correct"
pull against each other in this design space.

This document is a proposal. It does not change any code; each numbered
recommendation is meant to be picked up and implemented independently.

## What the codebase does today

flud's DHT is a custom Kademlia implementation (not a maintained library
like libp2p-kad) — see `flud/FludkRouting.py` for the routing table and
`flud/protocol/ClientDHTPrimitives.py` for the network protocol. Key
parameters: `k=12` (bucket depth, which also doubles as the replication
factor), `alpha=3` (lookup concurrency), a 256-bit ID space
(`FludkRouting.py:17-22`). Lookups follow the classic iterative FIND_NODE
algorithm (`ClientDHTPrimitives.py:_k_find_node_impl`), where each round is
a real HTTP request via aiohttp — every metadata read or write costs O(log
N) sequential network round trips before the actual store/fetch even
happens.

Two distinct kinds of metadata flow through the same primitives:

1. **Per-file block metadata.** Each file gets a content-derived storage key
   `sK = H(H(file))`, and its `blockMetadata` (locations of erasure-coded
   blocks) is written once via `k_store` (`FludFileOperations.py:632`).
   Because the key is content-addressed and the value is written once, this
   part of the design is naturally decentralization-friendly — it's exactly
   the workload Kademlia was designed for.

2. **A per-node manifest pointer.** Each node also stores a single CAS
   (content-address) value under its *own nodeID* as the DHT key
   (`FludFileOperations.py:_updateCAS`, lines 1519-1523). This is a classic
   **mutable pointer stored under a fixed key** — structurally identical to
   IPNS in IPFS — and it is the part of flud's design most exposed to DHT
   weaknesses around mutation and staleness.

   Worse, the manifest *content* itself isn't an incremental structure:
   every update re-encodes the **entire manifest file** (erasure-coded via
   `fludfilefec`) and republishes it as a brand-new object before swinging
   the CAS pointer to it (`UpdateManifest._store_manifest_async`,
   `FludFileOperations.py:1507-1523`). Backing up one additional file costs
   a full manifest rewrite, re-encode, and re-store — not an O(1) append.

Beyond the data model, three protocol-level choices compound the cost:

- **The write path has no partial-failure tolerance.** `k_store` fans out
  to all `k` nodes returned by the lookup and requires *every one* to
  succeed; a single unreachable replica fails the whole operation
  (`ClientDHTPrimitives.py:543-578`, `if failures: raise`). This is
  stricter than standard DHT/Dynamo-style semantics and directly hurts
  availability and tail latency as churn increases — the more nodes in the
  network, the more likely at least one of the k targets is briefly
  unreachable.
- **The read path has an ad hoc, unparameterized consistency policy.**
  `k_find_value` supports `first` (return on the first response — fast, no
  consistency guarantee) or `majority` (wait until one value has support
  from more than half of *observed* responses) via `_ValueAccumulator`
  (`ClientDHTPrimitives.py`). There is no read-repair: a majority read that
  detects a stale minority reply never corrects that replica.
- **There is no republish/TTL/refresh loop.** Classic Kademlia (and IPNS in
  practice) depends on periodically re-publishing owned/cached key-value
  pairs so replicas don't silently go stale or vanish as the network
  churns. flud's only periodic background loop
  (`FludNode._async_sync_loop`, `SYNCTIME=900`) persists local config; it
  does not refresh anything in the DHT. The recent `manifest checkpoint`
  rename in this repo's history (`523986b`) suggests this general area has
  already drawn attention.

Two smaller observations worth folding into any redesign:

- **Reputation tracking exists but isn't wired into routing.**
  `Reputation.py` / `TrustDeltas` adjust trust scores on VERIFY failures,
  but the lookup candidate ordering in `_LookupFrontier`
  (`ClientDHTPrimitives.py:298-337`) is pure XOR distance — reputation
  doesn't currently steer lookups away from flaky or adversarial peers.
- **There's already a working measurement harness.**
  `flud/protocol/DHTMetrics.py` and `flud/test/dht_benchmark.py` (which
  already supports adaptive alpha and a `first`/`majority` value policy
  toggle) can be used to validate any of the recommendations below with
  real before/after numbers rather than guesswork.

## Recent research grounding these recommendations

- **Optimistic Provide (IPFS/Kubo, 2024).** Return to the caller once the
  first successful write/ack lands, and keep propagating to the rest of the
  replica set in the background. This cut IPFS's publish latency from
  roughly 13-20s down to under 1s, and shipped as the default in Kubo
  0.39. ([ProbeLab](https://probelab.io/blog/optimistic-provide/))
- **Provide Sweep (IPFS, 2025).** Batches many keys destined for
  topologically-nearby DHT servers into shared routing-table walks,
  cutting DHT lookups from O(#keys) down to roughly O(#DHT servers / 20).
  Directly applicable to flud's dominant workload — a backup pass that
  stores many files/blocks in one run.
  ([ip.shipyard blog](https://ipshipyard.com/blog/2025-dht-provide-sweep/))
- **Kad-ReDS and related reputation-aware Kademlia routing.** Iterative
  peer-reputation tracking cuts lookup failure rates from roughly 21% down
  to 3-5% under 10-20% adversarial/churny populations, by biasing routing
  away from low-reputation peers instead of relying on pure XOR distance.
  ([FC'23 preproceedings](https://fc23.ifca.ai/preproceedings/130.pdf))
- **KadRTT and related lookup-parameter-tuning work.** Automatically
  derived alpha/timeout parameters reduce lookup latency versus static
  Kademlia defaults. flud's existing adaptive-alpha controller
  (`_AlphaController` in `ClientDHTPrimitives.py`) is already a step in
  this direction and could be extended with RTT-aware timeouts.
- **CRDT-based metadata synchronization (CrossFS and related work).**
  State-based CRDTs let concurrent metadata writers merge without
  coordination, achieving strong eventual consistency with low
  synchronization overhead across partitions — the right model for flud's
  manifest, which today has no defense against two concurrent writers
  racing on the same node's CAS pointer.
- **Willow protocol / Earthstar.** A purpose-built decentralized sync model
  using a (user, path, timestamp)-addressed data model with efficient
  partial sync and capability-scoped permissions. A useful reference shape
  for "manifest as a structured, partially-syncable log" rather than
  "manifest as a monolithic blob behind a single mutable pointer."
- **Vault: Decentralized Storage Made Durable
  ([arXiv:2310.08403](https://arxiv.org/abs/2310.08403)).** Rateless
  erasure coding combined with gossip-based, decentralized placement
  decouples durability from a synchronous k-of-k replica handshake, scaling
  past 10,000 nodes with near-ideal mean-time-to-data-loss at low
  redundancy. Relevant as a longer-term alternative to Kademlia's
  replicate-to-exactly-k model.
- **Scalability limits of Kademlia under high write throughput
  ([arXiv:2402.09993](https://arxiv.org/abs/2402.09993), on Ethereum Data
  Availability Sampling).** Documents concretely how synchronous
  k-of-k-style DHT writes become a seeding bottleneck under load —
  reinforcing the case for quorum writes over all-replicas writes.

## Recommendations

### A. Near-term, incremental, low risk (stay within the current Kademlia layer)

**Status: implemented.** All five items below are live in
`flud/protocol/ClientDHTPrimitives.py` (plus `flud/FludNode.py` for the
republish loop). Validate with `flud/test/dht_benchmark.py`'s
`--write-quorum`, `--read-quorum`, `--value-policy=quorum`, and
`--dht-latency-ms` flags together — see "Validating these changes" below.

**Bug found and fixed while validating A1.** Chaos-testing quorum writes
against a network with a killed node exposed a pre-existing, unrelated
correctness bug in the base Kademlia lookup: `_LookupFrontier.pending()`
and `has_better_pending()` excluded candidates already `queried` or
`in_flight`, but never candidates already recorded as `failed`. A node
that failed a request was removed from `in_flight` but never added to
`queried`, so it reappeared as "pending" and was re-queried on *every*
iteration of the lookup loop — indefinitely, since connection-refused
fails near-instantly and nothing else bounded the loop. In practice this
meant any `k_find_node`/`k_find_value`/`k_store` call touching a
routing-table-known peer that had actually gone offline would spin
forever, logging continuously (observed: ~70MB of log output per node in
under 25 seconds). This is normal peer churn for a P2P network, not an
edge case, and nothing in the prior test suite exercised "issue a lookup
after a known peer has already died," so it had gone undetected. Fixed by
threading `failed_ids` into both methods' blocked-set
(`ClientDHTPrimitives.py`); regression tests added in
`test_dht_scheduler_native.py`. This is the reason a literal "kill N node
processes, then store" benchmark doesn't actually exercise write-quorum
tolerance — a genuinely dead node is now filtered out during the FIND_NODE
lookup phase before the STORE fan-out ever begins. The scenario that
*does* exercise it is a node that stays reachable for lookups but fails
specifically on the STORE call, which is what A1's quorum write is for.

1. **Quorum writes instead of all-replicas writes.** `k_store` now succeeds
   once `W = floor(k/2) + 1` (or an explicit `write_quorum=`/
   `--write-quorum`) of the `k` target nodes ack, instead of requiring all
   `k`. Remaining in-flight stores continue as best-effort background tasks
   (tracked on `node._background_dht_tasks`, cancelled on node shutdown)
   rather than failing the caller. Mirrors Optimistic Provide's "ack early,
   finish in the background" pattern.
2. **Republish/refresh loop.** `FludNode._async_republish_loop` re-issues
   `k_store` every `FLUD_DHT_REPUBLISH_INTERVAL_S` (default 6h) for values
   this node owns: its manifest CAS pointer (`FludConfig.manifest_cas`, set
   by `UpdateManifest._updateCAS`) and any block metadata it originated,
   read back from the existing local cache files under `metadir`.
3. **Formalized (N, W, R) read consistency + read-repair.** `k_find_value`
   gained a `"quorum"` `value_policy` (alongside unchanged `"first"`/
   `"majority"`) with an explicit `read_quorum=`/`--read-quorum`, defaulting
   to `R = N - W + 1` (Dynamo-style `W + R > N`). When a majority/quorum
   read observes a stale minority value, the correct value is written back
   to those responders in the background (`_ValueAccumulator.stale_responders`).
4. **Client-side TTL cache.** `_TTLCache` on each `FludNode` (`node.dht_cache`,
   TTL via `FLUD_DHT_CACHE_TTL_S`, default 30s) short-circuits repeat
   `k_find_value` calls for the same key, and `k_store` proactively
   populates it — a `k_find_value` immediately following a `k_store` for
   the same key is a local cache hit, strengthening read-your-writes.
5. **Reputation-aware lookup ordering.** `_LookupFrontier`/`_candidate_sort_key`
   now demote candidates with negative scores in `FludConfig.reputations`
   (the actual live reputation store — `flud/Reputation.py` is unused dead
   code) behind all neutral-or-better candidates, without reordering among
   well-behaved nodes or penalizing nodes with no history.

### B. Medium-term, structural, still Kademlia-compatible

**Status: implemented.** Item 6 shipped as a git-tree-object-style
content-addressed **tree**, not the append-only hash-linked log originally
proposed above — that pivot happened mid-implementation and is worth
recording. A log with periodic checkpoints was the first design, but it
needed a checkpoint-interval tuning knob, `prev`-chain replay logic, and
still didn't give real piecemeal recovery below the checkpoint boundary.
A directory-tree structure (each directory is its own small content-addressed
object listing its children; only the root hash is a mutable pointer — the
same shape as a git tree object or IPFS UnixFS) gets everything the log
design was straining for and is simpler: O(depth) update cost instead of
O(checkpoint interval), true piecemeal recovery at any granularity (walk
from root, resolve one path component at a time), and no history-chain
bookkeeping at all. It also better matches `FludFileVersions.py`'s own
stated design philosophy (version history belongs in a local tool, not
flud's network layer) — the log design accidentally built a de facto
history mechanism nothing asked for; a pure current-state tree doesn't.
Per explicit decision, local storage was also fully unified with the DHT
representation (a local SQLite cache of the same tree nodes), replacing
`FludConfig`'s flat in-memory dict + full-file-rewrite-per-save scheme,
which the original maintainers had already flagged as not viable long
term (`FludConfig.py`, "this manifest all-in-mem scheme doesn't really
work long term").

6. **Manifest as a content-addressed tree, local storage unified with the
   DHT representation.** `FludConfig.py` holds the tree in a local SQLite
   db (`metadir/manifest.db`); `FludFileOperations.py`'s `StoreFile`
   writes into it locally (fast, offline, `manifest_tree_lock`-protected)
   on every file store, and `UpdateManifest`/`RetrieveManifest` (`PUTM`/
   `GETM`) publish/fetch it to/from the DHT — walking every locally-reachable
   node and `k_store`-ing each (cheap, idempotent — unchanged subtrees
   republish identical content under the same hash) instead of re-encoding
   the whole manifest as one erasure-coded blob on every publish. The
   published pointer is version-tagged (`{"fmt": 2, "root": <hash>}`) so a
   node can still recover manifests published before this migration via
   the original blob-based path. A node's existing flat-file manifest is
   auto-migrated to the tree once, locally, on first load after upgrading
   (the old file is kept as a `.pre-tree-migration` backup, not deleted).
   `flud-manifestViewer` and the `LIST` local-client command were rewritten
   to match (`LIST [path]`, local-vs-network modes for the viewer).
   **Two real, pre-existing bugs were found and fixed/documented along the
   way** (see "Bugs found" below) — the same pattern as tier A's chaos-testing
   discovery: exercising a code path for the first time surfaced latent
   issues nothing had ever hit before.
7. **Batch DHT operations for bulk workloads — implemented as concurrent
   client dispatch, not DHT-level batching (see below for why).**
   `AsyncLocalClient.sendPUTF` now flattens a directory subtree and
   dispatches file stores concurrently across a small pool of sibling
   connections (`sendPUTF(path, concurrency=8)`), instead of walking one
   directory level at a time and awaiting each file's full store
   sequentially. Measured **2.57x wall-clock speedup** (10 files,
   concurrency=8 vs. forced concurrency=1) — not full N-way, since
   `StoreFile`'s hashing/erasure-coding still run inline on flud's single
   `async_runtime` event-loop thread with no offload (a separate, pre-existing
   characteristic, not something this change fixes); the win is from
   overlapping the network-round-trip portions of concurrently-dispatched
   stores.

   **Scope note vs. the original proposal.** True DHT-level "Provide
   Sweep"-style batching (one node answering about multiple keys in a
   single request, actually reducing RPC count) was considered and
   explicitly descoped after investigation: `AsyncLocalClient.request()`
   already fully serializes each call's round trip under one lock, so a
   single client instance can't provide any real concurrency at all
   regardless of DHT-level changes — that had to be fixed first, and fixing
   it (via connection pooling) already captures most of the practical
   "bulk backups are faster" value on its own. The wire protocol also has
   no request-correlation ID, so genuine single-connection multiplexing
   isn't safe without a protocol change — a bigger, riskier lift than this
   scope called for. Left as a future "B7.5" if benchmarks ever show the
   remaining gap matters.

**Bugs found while implementing B6/B7** (pre-existing or introduced-then-caught,
unrelated to each other — exercising real code paths for the first time is
what surfaced them):
- **A bug in this implementation itself, caught by re-running the official
  suite after a port conflict cleared**: `manifest_tree_lock` (an
  `asyncio.Lock`) bound to whichever event loop was running the first time
  it was acquired, and raised `RuntimeError: ... is bound to a different
  event loop` on later use from a different one. `FludConfig` objects can
  legitimately outlive a single event loop — `conftest.py`'s session-scoped
  `flud_cluster` fixture reuses one node/config across many
  `test_fileop_native.py` test functions, each wrapping its calls in a
  fresh `asyncio.run()`. Fixed by tracking which loop the lock is bound to
  and recreating it on change (safe: asyncio guarantees only one loop runs
  per thread at a time, so a loop change means the old loop, and anything
  that might have contended for the old lock, is no longer running).
  Regression-tested (`test_manifest_tree_survives_reuse_across_separate_event_loops`).
- **A confirmed, concrete concurrency bug in the original flat-dict
  manifest**, caught during design review before it could ship: `AsyncLocal.py`
  configures `COMMAND_LIMITS["PUTF"] = 300` (up to 300 concurrent file
  stores is an intentional, already-used feature), and the old manifest
  update was safe only because it was fully synchronous with zero `await`
  points — a tree design that introduced yield points into that same
  critical section would have let concurrent sibling-file stores silently
  drop each other's directory entries. Fixed by design: `manifest_tree_lock`
  serializes the whole "resolve ancestor chain → write → propagate to
  root" sequence. Regression-tested (`test_manifest_tree_native.py`,
  25 concurrent sibling stores, all survive).
- **A latent concurrency race in `RetrieveFile`'s temp/download-file
  handling**, found while testing B6: `RetrieveFilename`'s directory
  retrieval gathers per-file `retrieve_file()` calls concurrently — a code
  path that could never actually run before, since the old flat-dict
  directory listing it replaced crashed on any real nested directory
  (matched all descendants at any depth, not just immediate children, and
  assumed every match was a file). Now that directory retrieval works,
  concurrent retrieval of *different* files occasionally produces "share
  header parse errors" / missing files under `clientdir` — apparent
  temp-file handling that isn't safe under concurrent multi-file retrieval.
  **Not fixed** (out of scope — it's in block-retrieval internals, a
  separate subsystem from manifest storage) but documented here as a
  known, real, reproducible issue worth its own investigation; the test
  suite works around it by testing storage/tree-walk correctness and
  retrieval correctness separately rather than relying on the concurrent
  path (see `test_manifest_tree_directory_walk_finds_all_nested_files`).

### C. Longer-term architectural rethink

8. **Make optimistic, asynchronous completion the default write shape
   everywhere**, not just block metadata — return once a quorum is
   durable, propagate the rest as a background task. This generalizes (A1)
   across the whole write path, consistent with the direction IPFS itself
   has moved.
9. **A checkpoint/consolidation strategy for block metadata**, directly
   addressing the "store metadata less frequently" angle from the original
   ask: instead of one `k_store` per file, accumulate a batch of
   block-metadata records locally and periodically publish a single
   Merkle-root checkpoint covering many files, with retrieval verified via
   a Merkle proof against that root. This trades per-file DHT write volume
   for a small increase in retrieval-time proof-verification cost, and is
   conceptually the same shift git made from "one object per change" to
   "commits over a checkpoint history" — echoed by the `manifest
   checkpoint` rename already present in this repo's history.
10. **Evaluate a non-synchronous-replication durability model**, following
    Vault's rateless-erasure-code-plus-gossip placement approach, as a
    longer-horizon alternative to "replicate to exactly k nodes and require
    a quorum to ack synchronously." This is a substantially larger lift — a
    new placement/durability protocol, not a tweak to the existing one —
    and should only be pursued if (A) and (B) prove insufficient at target
    scale. It's listed here as the "if incremental isn't enough" option,
    not a first move.

## Consistency and atomicity tradeoffs

- **N-of-N → W-of-N writes** trades a strict "every replica has it before
  we return" guarantee for availability and lower tail latency. Readers
  must compensate with an explicit `R` such that `W + R > N` to preserve
  the read-your-writes property flud implicitly relies on today — for
  example, storing a file and then immediately updating or reading the
  manifest.
- **The manifest CAS pointer is the system's only truly mutable, contended
  key.** Recommendation B6 (the CRDT/append-only log) is what actually
  fixes atomicity for concurrent writers. The quorum and read-repair
  changes in (A) make individual reads and writes more robust, but they do
  not resolve a genuine write-write race on the same pointer — two
  processes updating the same node's manifest concurrently today can still
  race under last-writer-wins semantics even after those changes.
- **Checkpointing/consolidation (C9) reduces write volume but increases the
  "blast radius"** of losing a checkpoint, and increases the latency of
  proving any single file's metadata (a proof against a larger structure
  instead of a direct lookup). Any implementation should treat the
  checkpoint interval as an explicit, tunable tradeoff rather than a fixed
  constant, since the right interval depends on write volume, node churn
  rate, and how much staleness callers can tolerate between checkpoints.

## Validating these changes

`flud/test/dht_benchmark.py` and `flud/protocol/DHTMetrics.py` already
exist for exactly this purpose — they instrument round counts, RPC
attempts/failures, alpha samples, and store/read outcomes per operation.
Any of the recommendations above should be validated by running
`make benchmark-dht` (or `poetry run python3 flud/test/dht_benchmark.py`
directly, which also exposes `--alpha`, `--alpha-mode`, and
`--value-policy` knobs) before and after the change, on both cold and warm
routing-table states, so the impact is measured rather than assumed.

The benchmark's emulated network otherwise runs entirely on loopback
(sub-millisecond RTT), which hides the multiplicative effect DHT changes
have on round-trip-bound latency in a real, WAN-connected network. The
harness now supports `--dht-latency-ms` (fixed, or a `MIN-MAX` range
resolved once per node for a heterogeneous/asymmetric network) and
`--dht-latency-jitter-ms`, which inject artificial delay server-side into
DHT RPCs only (FIND_NODE/FIND_VALUE/STORE — file transfer is unaffected).
Run the benchmark with and without these flags to see how a change
performs under realistic RTT, not just at loopback speed; see
`flud/test/README.md` for details. The same injection is available for
manual testing via `start-fludnodes`, driven by the underlying
`FLUD_SIM_DHT_LATENCY_MS` / `FLUD_SIM_DHT_JITTER_MS` environment variables.
