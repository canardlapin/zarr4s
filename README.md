# zarr4s

zarr4s is a portable Scala 3 implementation of Zarr v3 with read-only support
for common Zarr v2 arrays and groups. The `zarr4s-core` artifact provides
runtime-rank array metadata, chunk planning, codec programs, object-store
capabilities, readers, create-only writers, and bounded caches.

The same API runs on the JVM and Scala.js. The name follows the Scala `*4s`
convention; it does not refer to a Zarr format version.

```scala
import zarr4s.*
```

## Modules and development

- `zarr4s-core` contains the dependency-free JVM/Scala.js kernel.
- `zarr4s-codec-blosc-zstd` is an optional JVM/Scala.js codec provider.

The artifacts are not published yet. To verify a source checkout:

```bash
npm ci --prefix codec-blosc-zstd/js
sbt checkAll
```

`checkAll` checks formatting, compiles every module, and runs every JVM and
Scala.js test. A sibling source build can be consumed with sbt `ProjectRef`s
until immutable GitHub revisions and published artifacts are available.

## 0.1 boundary

The kernel supports Zarr v3 groups and arrays; read-only lowering of common v2
arrays and groups into the same descriptor; explicit hierarchy navigation;
bounded v2 `.zmetadata` and v3 inline consolidated indexes; runtime ranks
including zero; regular chunk grids; default and v2-compatible chunk keys;
Boolean values; every signed and unsigned integer width from 8 through 64 bits;
float16; float32; float64; complex64; complex128; bounded raw-width `rN`
carriers; C and Fortran order through normative transpose;
little/big-endian byte encoding; the common Zarr v2 shuffle filter; chunk-local gzip and
Zarr v2 zlib; CRC32C; and start/end
`sharding_indexed` reads and writes. Shared code provides deterministic
create-only v3 array and group writers over synchronous or asynchronous object
capabilities. The same shared `AsyncZarr` reader runs on the JVM and Scala.js;
`BrowserZarr` remains a source-compatible facade that selects browser gzip and
zlib by default. JVM adds confined filesystem and checked HTTP range stores, an
explicit blocking-reader adapter whose blocking execution context is supplied
at construction, and atomic staged-directory publication.
An explicit blocking-codec adapter similarly lets portable async interpreters
use a synchronous JVM codec only on a caller-supplied dedicated execution
context; blocking work is never smuggled onto the callback context.
Scala.js adds Fetch range reads and browser gzip/zlib; a caller-supplied
`AsyncObjectWriter` provides the write transport without a cloud SDK in core.

Selections are runtime-rank and factored by axis. `All`, positive-step `Slice`,
and ordered `Indices` selectors compose with Cartesian/orthogonal semantics;
unsorted indices and duplicates are preserved. The planner groups each axis
independently and shares those factors across the chunk product, so a
`time × masked-voxel` request does not become an in-memory list of every
time/voxel coordinate. Descending slices currently refuse explicitly.

```scala
val selected = for
  time <- AxisSelector.slice(0L, 1200L, step = 2L)
  voxels <- AxisSelector.indices(maskedVoxelIds*)
  selection <- FactoredSelection(boldShape, time, voxels)
yield selection
```

`read(selection)` materializes through the same fragment interpreter exposed
by `foldFragments` and `foreachFragment`. Each `ChunkFragment` contains only
the selected values from one logical chunk plus an explicit output placement.
Sync folds stop before the next store request; async folds await the returned
`Future`, providing bounded backpressure without a streaming-framework
dependency. `FragmentReceipt` separates index/data bytes, decoded/fill chunks,
emitted fragments/elements, completion, and read amplification.

Remote reuse is an explicit capability rather than a process global:

```scala
val cached = for
  revision <- CacheNamespace.from("manifest:blake3:8d7b...")
yield CachingAsyncObjectReader(
  remoteStore,
  ObjectReadCache(revision, CacheLimits.default)
)
```

An `ObjectReadCache` cannot exist without a caller-supplied immutable revision
identity. It stores exact whole objects, ranges, and lengths; a requested range
may reuse a cached containing range or whole object. Entries are bounded by
both bytes and count, evicted by deterministic LRU order, and copied at cache
ingress and egress. The async decorator collapses identical in-flight requests
but never retains a typed store error or failed `Future`. `CacheStats` reports
hits, misses, downstream requests, fetched/served/evicted bytes, resident size,
and single-flight joins. There is deliberately no global cache, TTL, retry,
persistence, credential, or prefetch policy. `ReadLimits.maxConcurrentRequests`
remains the sole reader scheduling bound.

Writing is a capability, not a filesystem assumption:

```scala
val written = ZarrPath("subjects/sub-01/bold").map: path =>
  SyncZarrWriter.create(
    store = objectWriter,
    descriptor = array,
    provider = chunks,
    path = path
  )
```

`ObjectWriter.create` and `AsyncObjectWriter.create` create one immutable
object and refuse replacement. The interpreters visit chunks in deterministic
grid order, omit declared fill chunks, bound one encoded chunk or shard at a
time, and create `zarr.json` last. `WriteOutcome.Complete` therefore has a
completion marker. `WriteOutcome.Incomplete` retains the exact objects, bytes,
logical chunks, fill/padding omissions, and typed error seen before an
object-store interruption; it never implies namespace rollback. Async writing
waits for each provider, codec, and object effect before advancing, providing
portable backpressure. `JvmZarrWriter` strengthens this base contract by
cleaning a private stage and atomically moving the completed directory.

Every written object and the metadata marker carry a portable SHA-256 identity
in `WriteReceipt`. The same interpreter handles scalar, empty, and arbitrary
rank arrays, all built-in fixed-width carriers, transpose, default and v2 chunk
keys, CRC32C, platform gzip/zlib, optional byte-codec providers, and start/end
indexed sharding. It deliberately provides no overwrite, resize, append, or
concurrent-mutation operation.

`ConsolidationMode` makes index policy explicit. A caller can prefer, require,
or ignore consolidation. Explicit paths remain navigable when an index is
absent. Synchronous groups can discover un-consolidated children when the
caller supplies an `ObjectLister`; asynchronous groups expose the same
operation through `discoverChildren` and `AsyncObjectLister`. Memory and JVM
filesystem stores provide listing, while HTTP and Fetch remain caller-provided
capabilities. Without a listing capability, child enumeration refuses
honestly. V3 inline consolidation is treated as an optional interoperability
extension rather than a claim that it is part of the normative v3
specification.

It intentionally does not own scientific-domain profiles, mutation, v2
writing, S3 credentials, persistent caches, prefetch or retention policy,
Blosc itself, or every extension. Variable-length, structured values, v2
object arrays, and v2 filters beyond shuffle are not yet claimed. Unsupported
metadata crosses a typed error boundary instead of being guessed at.

The core has no production library dependency. Its runtime-rank values,
immutable descriptors, pure planners, explicit codec and store capabilities,
and JVM/Scala.js interpreters are suitable for scientific workloads without
embedding a particular array rank or domain model.

Codec extension has two explicit halves. A `CodecCapability` validates JSON
and compiles it into a stage; a `SyncByteCodecExecutor` or
`AsyncByteCodecExecutor` supplies that stage's algorithm at the IO boundary.
`CodecProgram` makes representation transitions lawful before a descriptor can
open. Immutable runtimes reject duplicate providers and report a known schema
with a missing executor as a typed capability error. This keeps optional JNI,
WASM, or JavaScript codecs in separate provider artifacts without adding a
match branch or dependency to the kernel.

Data types follow the same principle without dispatching on names in the hot
path. A `DataTypeCapability` owns exact fill parsing and selects a `ScalarKind`;
that closed carrier algebra owns primitive-block compatibility, allocation,
assembly, and endian serialization. A downstream fixed-width type can reuse a
lawful carrier (for example, a constrained unsigned byte) without teaching the
kernel its identifier. Array-to-array codecs expose explicit shape laws, so
transpose composes with bytes and compression at arbitrary rank on both
platforms.

The optional `zarr4s-codec-blosc-zstd` module extends that seam with Blosc/Zstd
and standalone Zstandard codecs. It leaves this kernel dependency-free while
testing native JVM and pure-JavaScript implementations on Scala.js.

The executable architecture and extraction gates are recorded in
[`docs/plans/zarr-z6-codec-architecture.md`](docs/plans/zarr-z6-codec-architecture.md).
The common-feature support matrix and conformance courts are recorded in
[`docs/plans/zarr-z9-common-zarr-support.md`](docs/plans/zarr-z9-common-zarr-support.md).
The repository also contains a deliberately independent JVM/Scala.js consumer
under [`examples/standalone-consumer`](examples/standalone-consumer).
Interoperability tests read both Zarr-Python fixtures and an attributed fixture
written by the independent [`zarrs`](https://github.com/zarrs/zarrs) Rust
implementation. A second pinned fixture comes from the official
[`zarr_implementations`](https://github.com/zarr-developers/zarr_implementations)
compatibility corpus and exercises a Zarr v2 group, dot chunk key, uint8 data,
and gzip on both platforms. Its older draft-v3 material is not presented as
current Zarr v3 conformance.

The opt-in [`tools/zarr-java-oracle`](tools/zarr-java-oracle) project
provides a reciprocal zarr-java 0.1.3 gate without entering the Scala module
graph: zarr-java opens Scala-published arrays, while the shared JVM/Scala.js
suite reads a SHA-pinned direct v3 fixture written by zarr-java. The tool's
heavy transitive JVM graph is therefore evidence, not a core dependency.

The original extraction receipt remains in the
[`canardlapin/scalafim`](https://github.com/canardlapin/scalafim/blob/main/docs/benchmarks/zarr-z8-profile-extraction.md)
repository.

Reads return an `ExecutionReceipt` containing actual object/range/length
requests, index and data bytes, requested logical bytes, touched chunks/shards,
and read amplification. Writes return complete or incomplete receipts with
portable content hashes and physical counters. Diagnostics are values; the
kernel does not log or hide timing, retry, scheduling, or cache policy in
globals.
