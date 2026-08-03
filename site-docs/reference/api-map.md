# Public API reference

Use this page after the task guides. It points from a user question to the
smallest relevant public entry point; it does not repeat symbol-level
Scaladoc.

## Browse Scaladoc

The API reference is generated separately for each public module and platform.
Shared symbols appear in both platform trees; adapters such as `JvmZarr` and
`BrowserZarr` appear only where they can run.

| Module | JVM | Scala.js |
| --- | --- | --- |
| Core arrays, stores, readers, and writers | [JVM Scaladoc](https://canardlapin.github.io/zarr4s/api/core/jvm/) | [Scala.js Scaladoc](https://canardlapin.github.io/zarr4s/api/core/js/) |
| Optional Blosc and Zstandard provider | [JVM Scaladoc](https://canardlapin.github.io/zarr4s/api/codec-blosc-zstd/jvm/) | [Scala.js Scaladoc](https://canardlapin.github.io/zarr4s/api/codec-blosc-zstd/js/) |

## Create and own values

| Task | Entry point | Result or consequence |
| --- | --- | --- |
| Construct a runtime shape | `Shape(dimensions*)` | `Either[ZarrError, Shape]` |
| Describe a typed array | `ArraySpec(dtype, shape, chunkShape)` | Checked `ArraySpec[D]`, defaulting to v3. |
| Add fill, names, attributes, or v2 format | `withFill`, `withDimensionNames`, `withAttributes`, `asFormat` | A new immutable specification; dimension names recheck rank. |
| Copy Scala primitive values | `DenseArray.copyOf` | Owned typed dense data; input is copied. |
| Transfer an already-owned array | `DenseArray.adopt` | No input copy; caller must stop mutating the source. |

## Open and read

| Task | Synchronous | Asynchronous or browser |
| --- | --- | --- |
| Open a dtype-known array | `SyncZarr.openTypedArray` | `AsyncZarr.openTypedArray`, `BrowserZarr.openTypedArray` |
| Open a dynamic array | `SyncZarr.openArray` | `AsyncZarr.openArray`, `BrowserZarr.openArray` |
| Open a group or node | `SyncZarr.openGroup`, `openNode` | Async/browser counterparts |
| Open an existing JVM directory | `JvmZarr.openTypedArray`, `openArray`, `openGroup`, `openNode` | Supplies filesystem listing automatically. |
| Read the full array | `TypedOpenedArray.readAll` | `AsyncTypedOpenedArray.readAll` |
| Read a rectangle | `readRegion(Region)` | Same method returning a `Future` |
| Read points | `readPoints(CoordinateBatch)` | Same method returning a `Future` |
| Slice/gather each axis | `read(FactoredSelection)` | Same method returning a `Future` |
| Fold decoded fragments | `asOpenedArray.foldFragments` | Async fragment fold on the dynamic handle |

Typed reads return `TypedReadResult[D]`, which contains owned `DenseArray[D]`
data and an `ExecutionReceipt`.

## Create arrays and groups

| Task | Entry point | Important contract |
| --- | --- | --- |
| Create from dense values | `SyncZarr.createArray`, `AsyncZarr.createArray` | New objects only; result retains complete or incomplete outcome. |
| Create and reopen | `createAndOpenArray` | Adds a typed handle only after complete publication. |
| Create a fill-only array | `createFillArray` | Omits physical fill chunks. |
| Stream chunks | `createArrayFromProvider` | Requires a `TypedChunkProvider[D]`. |
| Publish a new JVM directory | `JvmZarr.createArray` or `createAndOpenArray` | Staged filesystem publication to a `Path`. |
| Create a group | `SyncZarr.createGroup`, `AsyncZarr.createGroup`, `JvmZarr.createGroup` | Uses `GroupSpec` and retains complete or incomplete progress. |

`TypedWriteResult[D]` contains the specification, compiled descriptor, and
`WriteOutcome`. `WriteOutcome.Complete` contains a `WriteReceipt`;
`Incomplete` contains progress and the failure.

## Stores and remote-read controls

| Need | API |
| --- | --- |
| Deterministic test or transient store | `MemoryStore`, `AsyncMemoryStore` |
| Existing filesystem directory | `JvmFileStore.openChecked` for typed errors; `open` for legacy string errors. |
| HTTP range reads on JVM | `JvmHttpStore` |
| Browser Fetch reads | `FetchStore` |
| Custom backend | Implement `ObjectReader`/`ObjectWriter` or async counterparts. |
| Group discovery without consolidation | Supply `ObjectLister` or `AsyncObjectLister`. |
| Revision-scoped LRU cache | `ObjectReadCache` plus `CachingObjectReader` or `CachingAsyncObjectReader`. |
| Bound one operation | `OpenLimits`, `ReadLimits`, `WriterLimits`, `CacheLimits` |

## Codecs

Use `ArrayCodecSpec` and `ShardingSpec` for programmatic built-in encoding.
Use `ZarrCapabilities` to compile supported extension metadata and
`SyncCodecRuntime` or `AsyncCodecRuntime` to supply the corresponding
executors. Metadata support without an executor remains a typed failure.

The source for these contracts remains available in
[`core`](https://github.com/canardlapin/zarr4s/tree/main/core). The
[`standalone consumer`](https://github.com/canardlapin/zarr4s/tree/main/examples/standalone-consumer)
checks the ordinary public imports independently of the generated reference.

Next: [diagnose common failures](../help/troubleshooting.md).
