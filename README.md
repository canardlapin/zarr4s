# zarr4s

[![CI](https://github.com/canardlapin/zarr4s/actions/workflows/ci.yml/badge.svg)](https://github.com/canardlapin/zarr4s/actions/workflows/ci.yml) · [Apache-2.0](LICENSE) · [Design notes](docs/README.md) · [Standalone consumer](examples/standalone-consumer/)

`zarr4s` is a cross-platform Scala 3 library for creating, reading, and
validating Zarr arrays and groups on the JVM and Scala.js. Use it when you need
a portable Zarr kernel that keeps dtype, shape, ownership, capabilities, and
I/O errors explicit. The typed façade covers the ordinary dense-array path;
descriptor and provider APIs remain available for streaming data and custom
storage.

The default artifact is `zarr4s-core`. Its shared core is dependency-free at
runtime and cross-compiles to both platforms; JVM filesystem/HTTP transports,
browser Fetch, and optional codecs live at explicit platform or provider
boundaries.

> **Status:** 0.1 pre-release. No stable release artifact is published yet;
> use a checkout or the local publication path in the standalone-consumer
> example.

## Quick start

The typed façade makes the common path explicit without making callers write
metadata JSON or construct chunk providers. `ArraySpec[D]` checks the dtype,
shape, chunk shape, and optional format choices; `DenseArray.copyOf` copies a
regular Scala array once and owns the resulting storage. Every validation and
I/O boundary remains an `Either[ZarrError, *]`, so applications can choose
their own error policy.

The following is the complete shared example. It writes a 2 × 3 `int16` Zarr
v3 array to an in-memory store and reads it back through a typed handle. The
same source compiles on the JVM and Scala.js.

```scala
import zarr4s.*

@main def quickstart(): Unit =
  val result =
    for
      shape <- Shape(2L, 3L)
      chunks <- Shape(2L, 3L)
      spec <- ArraySpec(DType.Int16, shape, chunks)
      values <- DenseArray.copyOf(
        DType.Int16,
        shape,
        Array[Short](1, 2, 3, 4, 5, 6)
      )
      store <- MemoryStore.empty
      created <- SyncZarr.createAndOpenArray(store, spec, values)
      opened <- created.opened
      read <- opened.readAll()
    yield read

  result match
    case Right(read) =>
      println(s"values = ${read.data.toArray.toVector}")
      println(s"bytes read = ${read.receipt.bytesRead}")
    case Left(error) => throw IllegalArgumentException(error.message)
```

The final match is the application’s error policy; there is no library-wide
throwing or retry policy hidden in the façade. `read.data` is an owned
`DenseArray[DType.Int16.type]`, and `read.receipt` is the actual
`ExecutionReceipt` for this read.

Expected output:

```text
values = Vector(1, 2, 3, 4, 5, 6)
bytes read = 12
```

## What it covers

- Create and read typed dense arrays without writing metadata JSON or a custom
  chunk provider.
- Open common Zarr v2 and v3 arrays and groups through one validated descriptor
  model.
- Read complete arrays, regions, points, and factored selections, including
  border chunks, fill values, and indexed sharding.
- Run the shared kernel on the JVM and Scala.js while supplying storage,
  scheduling, retry, cache, and codec capabilities explicitly.
- Keep optional Blosc/Zstandard support out of the core artifact until an
  application opts into the separate provider.

## Choose an entry point

For a writer-only capability, use `SyncZarr.createArray` (or
`createArrayFromProvider`) and inspect the returned `TypedWriteResult`. Its
`WriteOutcome.Complete` contains a `WriteReceipt`; `WriteOutcome.Incomplete`
retains progress and the typed error. The writer is create-only: it will not
overwrite an existing target, append, resize, or publish an incomplete target.

`DenseArray.copyOf` is the safe boundary for an ordinary Scala array. Use
`DenseArray.adopt` only when the caller transfers ownership and will never
mutate the supplied array afterward. Shapes are runtime-rank values, so
invalid rank, chunk, and element-count combinations fail during construction.
The `DType` witness then refines an opened dynamic descriptor and prevents a
read from silently changing representation.

The defaults are deliberately small: regular chunks, little-endian bytes for
direct v3 arrays, no compression, and caller-supplied object-store and
scheduling capabilities. Opt into a supported codec or indexed sharding
explicitly:

```scala
val sharding = ShardingSpec.indexed(Shape(1L, 3L).toOption.get)
val created = SyncZarr.createAndOpenArray(
  store,
  spec,
  values,
  sharding = Some(sharding),
  codecs = Vector(ArrayCodecSpec.Crc32c)
)
```

Codec availability is a capability, not an implicit global. Supply the
platform runtime (for example `JvmCodecRuntime.portable` or
`BrowserCodecRuntime.portable`) and any optional provider required by the
chosen codec; unsupported combinations return `ZarrError`.

For a typed sub-region, construct the region explicitly and keep the same
typed result and receipt:

```scala
val subregion = for
  origin <- Coordinate(0L, 0L)
  extent <- Shape(1L, 3L)
  region <- Region.within(spec.shape, origin, extent)
yield region

val selected = subregion.flatMap(opened.readRegion(_))
```

The low-level descriptor/provider layer remains the right tool for streaming
data, custom chunk construction, external metadata, or fragment-level reads.
It is an advanced route, not a deprecated one:

<details>
<summary>Advanced: metadata and custom chunk providers</summary>

```scala
val metadata =
  """{
    |  "zarr_format": 3,
    |  "node_type": "array",
    |  "shape": [2, 3],
    |  "data_type": "int16",
    |  "chunk_grid": {
    |    "name": "regular",
    |    "configuration": {"chunk_shape": [2, 3]}
    |  },
    |  "chunk_key_encoding": {
    |    "name": "default",
    |    "configuration": {"separator": "/"}
    |  },
    |  "fill_value": 0,
    |  "codecs": [
    |    {"name": "bytes", "configuration": {"endian": "little"}}
    |  ],
    |  "dimension_names": ["y", "x"],
    |  "attributes": {},
    |  "storage_transformers": []
  }""".stripMargin

val descriptor = ZarrMetadata.parse(metadata).flatMap:
  case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array)
  case ZarrNodeMetadata.Group(_)     => Left(ZarrError.UnsupportedNodeType("group"))

val block = PrimitiveBlock.Int16(
  OwnedShorts.copyOf(Array[Short](1, 2, 3, 4, 5, 6))
)

val provider = new ChunkProvider:
  def chunk(
      coordinate: ChunkCoordinate,
      storedShape: Shape
  ): Either[ZarrError, ChunkPayload] =
    // Stream or synthesize the requested chunk here.
    Right(ChunkPayload.Values(block))

val written = for
  foundDescriptor <- descriptor
  foundStore <- MemoryStore.empty
  outcome <- SyncZarrWriter.create(foundStore, foundDescriptor, provider).toEither
yield outcome
```

The advanced path is where a caller supplies canonical metadata, a custom
provider, or fragment-level policy. It still uses the same create-only writer,
typed errors, capability checks, and receipts as the façade.

</details>

On the JVM, `JvmZarr.createAndOpenArray(target, spec, values)` adds an explicit
`java.nio.file.Path` boundary and stages publication atomically. In a browser,
`BrowserZarr.createAndOpenArray(store, spec, values)` delegates to the async
kernel with `BrowserCodecRuntime.portable`; callers still supply an
`ExecutionContext`, object capabilities, limits, and any retry or cache policy.

For a separate JVM/Scala.js consumer build, see
[`examples/standalone-consumer`](examples/standalone-consumer).


## Modules and platforms

| Module or platform | Provides |
| --- | --- |
| `zarr4s-core` | Shared metadata, planning, readers, writers, stores, caches, and codec runtime. |
| JVM portion of `core` | Confined filesystem and checked HTTP range stores, blocking adapters, and atomic staged-directory publication. |
| Scala.js portion of `core` | Fetch range reads, browser gzip/zlib, and the `BrowserZarr` facade. |
| `zarr4s-codec-blosc-zstd` | Optional Blosc and standalone Zstandard providers for JVM and Scala.js. |

The name follows the Scala `*4s` convention; it does not refer to a Zarr
format version. Core has no cloud SDK, JNI, WebAssembly, or scientific-domain
dependency.

## API at a glance

| Concern | Main API | Contract |
| --- | --- | --- |
| Typed create/read | `ArraySpec`, `DenseArray`, `SyncZarr`, `AsyncZarr` | Checked dtype/shape intent, owned values, typed handles, create-only outcomes, and execution receipts. |
| Metadata | `ZarrMetadata`, `ArrayDescriptor` | Parse and compile supported metadata into typed descriptors; unsupported required metadata returns `ZarrError`. |
| Reading | `SyncZarr`, `AsyncZarr`, `TypedOpenedArray` | Open arrays and groups, read all/regions/points, and materialize or fold selected fragments. |
| Writing | `SyncZarr`, `AsyncZarr`, `SyncZarrWriter`, `AsyncZarrWriter` | Create arrays and groups without overwrite, resize, append, or concurrent mutation. |
| Storage | `ObjectReader`, `ObjectWriter` and async variants | Callers provide object and listing capabilities; the core does not assume a filesystem or cloud service. |
| Caching | `CachingObjectReader`, `CachingAsyncObjectReader` | Revision-scoped, bounded, deterministic LRU reuse with no global cache policy. |
| Codecs | `CodecCapability`, codec executors, `CodecProgram` | Metadata validation and execution are separate; providers supply platform algorithms explicitly. |

## Supported scope

The current 0.1 line supports:

- Zarr v3 arrays and groups, plus common Zarr v2 arrays and groups lowered to
  the same descriptor model.
- Explicit hierarchy navigation, bounded v2 `.zmetadata`, and v3 inline
  consolidated indexes.
- Runtime-rank arrays, including rank zero; regular chunk grids; default and
  v2-compatible chunk keys; and `sharding_indexed` reads and writes.
- Boolean values; every signed and unsigned integer width from 8 through 64
  bits; `float16`, `float32`, `float64`, `complex64`, `complex128`; and bounded
  raw-width `rN` carriers.
- C and Fortran order through normative transpose, little- and big-endian byte
  encoding, CRC32C, gzip, Zarr v2 zlib, common v2 shuffle, and dtype-aware
  delta filters for fixed-width boolean, integer, and floating arrays.
- Synchronous and asynchronous object capabilities, with portable backpressure
  and caller-supplied scheduling limits.

To publish v2 metadata, pass `format = ZarrFormat.V2` to a writer. The writer
uses normative v2 chunk keys and creates the final `.zarray` or `.zgroup`
completion marker after the preceding objects.

### Selections

Selections are runtime-rank and factored by axis. `All`, positive-step `Slice`,
and ordered `Indices` selectors use Cartesian/orthogonal semantics; unsorted
indices and duplicates remain in the requested order.

<details>
<summary>Factored selection example</summary>

```scala
val selected = for
  arrayShape <- Shape(1200L, 100L)
  firstAxis <- AxisSelector.slice(0L, 1200L, step = 2L)
  secondAxis <- AxisSelector.indices(3L, 11L, 42L, 11L)
  selection <- FactoredSelection(arrayShape, firstAxis, secondAxis)
yield selection
```

</details>

`selected` is an `Either[ZarrError, FactoredSelection]`. Pass the successful
selection to `OpenedArray.read`; the duplicate index `11` is preserved.
`foldFragments` and `foreachFragment` expose the same chunk-local interpreter
with explicit output placement and bounded synchronous or asynchronous
backpressure.

### Publication, caching, and diagnostics

- A writer visits chunks in deterministic grid order, omits declared fill
  chunks, bounds one encoded chunk or shard at a time, and reports complete or
  incomplete progress. Generic object stores do not promise namespace rollback.
- `WriteReceipt` records written objects, byte counts, omitted fill or padding
  chunks, and portable SHA-256 identities.
- A cache requires a caller-supplied immutable revision identity. It stores
  exact objects and ranges, is bounded by bytes and entry count, uses
  deterministic LRU eviction, and does not retain typed store errors or failed
  futures.
- `ExecutionReceipt` reports object, range, and length requests; index and data
  bytes; requested logical bytes; touched chunks or shards; and read
  amplification. Timing, retry, scheduling, credentials, and retention policy
  remain caller concerns.

### Explicit boundaries

The core does not own scientific-domain profiles, S3 credentials, persistent
caches, prefetch or retention policy, mutation, or every Zarr extension.
Variable-length and structured values, v2 object arrays, and v2 filters beyond
shuffle and dtype-aware delta are not claimed. Unsupported metadata fails
through `ZarrError` instead of being guessed at. Descending slices are rejected.

## Documentation map

- [Standalone consumer](examples/standalone-consumer/) — compile and run a
  public-artifact JVM/Scala.js consumer.
- [Design and verification records](docs/README.md) — current guarantees,
  support decisions, and historical measurements.
- [Common Zarr support](docs/plans/zarr-z9-common-zarr-support.md) — supported
  metadata, selection, store, and codec boundaries.
- [Typed façade evidence](docs/plans/zarr-z10-evidence.md) — executable
  cross-platform, consumer, and interoperability evidence.
- [Optional codec provider](codec-blosc-zstd/README.md) — explicit Blosc and
  Zstandard setup and platform caveats.

## Interoperability and verification

- The common support matrix and conformance gates are recorded in
  [`docs/plans/zarr-z9-common-zarr-support.md`](docs/plans/zarr-z9-common-zarr-support.md).
- Codec architecture and extraction gates are recorded in
  [`docs/plans/zarr-z6-codec-architecture.md`](docs/plans/zarr-z6-codec-architecture.md).
- Interoperability suites read Zarr-Python fixtures, an attributed fixture from
  [`zarrs`](https://github.com/zarrs/zarrs), and a pinned v2 fixture from the
  official [`zarr_implementations`](https://github.com/zarr-developers/zarr_implementations)
  corpus. The corpus's older draft-v3 material is not presented as current v3
  conformance.
- Optional external readback gates use
  [`tools/verify_zarr_python_interop.py`](tools/verify_zarr_python_interop.py),
  [`tools/verify_zarrs_v2_interop.py`](tools/verify_zarrs_v2_interop.py), and
  [`tools/zarr-java-oracle`](tools/zarr-java-oracle).
- The original extraction receipt remains in
  [`canardlapin/scalafim`](https://github.com/canardlapin/scalafim/blob/main/docs/benchmarks/zarr-z8-profile-extraction.md).

## Development

Run the focused cross-platform gate from the repository root:

```bash
sbt coreJVM/test coreJS/test
```

Run the full gate, including the optional codec provider:

```bash
npm ci --prefix codec-blosc-zstd/js
sbt checkAll
```

`checkAll` checks formatting, compiles every module, and runs every JVM and
Scala.js test. To verify that an external consumer can compile against local
artifacts:

```bash
sbt 'coreJVM/publishLocal' 'coreJS/publishLocal'
cd examples/standalone-consumer
sbt 'consumerJVM/compile' 'consumerJS/compile'
```

The optional provider has its own setup instructions in
[`codec-blosc-zstd/README.md`](codec-blosc-zstd/README.md). Design and
verification notes are indexed in [`docs/README.md`](docs/README.md).

## License

`zarr4s` is released under the [Apache License 2.0](LICENSE).
