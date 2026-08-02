# zarr4s

`zarr4s` is a Scala 3 implementation of Zarr v3 with create-only support for
common Zarr v2 arrays and groups. The shared API cross-compiles to the JVM and
Scala.js.

The `zarr4s-core` artifact is dependency-free. It supplies metadata validation,
runtime-rank array descriptors, chunk planning, readers, writers, object-store
capabilities, codec programs, and bounded caches. Platform code supplies JVM
filesystem and HTTP transports or browser Fetch; optional codecs live in a
separate artifact.

> Current status: early development on the 0.1 line. Artifacts are not
> published yet.

## Quick start

The following example writes a 2 × 3 `int16` Zarr v3 array to an in-memory
store, then reads it back. It uses only `zarr4s-core`, so the shared code works
on both platforms.

<details>
<summary>Complete in-memory example</summary>

```scala
import zarr4s.*

@main def quickstart(): Unit =
  def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => throw IllegalArgumentException(error.message)

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
      |}""".stripMargin

  val descriptor = value(
    ZarrMetadata.parse(metadata).flatMap:
      case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array)
      case ZarrNodeMetadata.Group(_)     => Left(ZarrError.UnsupportedNodeType("group"))
  )

  val values = PrimitiveBlock.Int16(
    OwnedShorts.copyOf(Array[Short](1, 2, 3, 4, 5, 6))
  )
  val provider = new ChunkProvider:
    def chunk(
        _coordinate: ChunkCoordinate,
        _storedShape: Shape
    ): Either[ZarrError, ChunkPayload] = Right(ChunkPayload.Values(values))

  val store = value(MemoryStore(Map.empty))
  SyncZarrWriter.create(store, descriptor, provider).toEither match
    case Left(error) => throw IllegalArgumentException(error.message)
    case Right(_)    => ()

  val opened = value(SyncZarr.openArray(store))
  val origin = value(Coordinate(0L, 0L))
  val region = value(Region.within(descriptor.shape, origin, descriptor.shape))
  val result = value(opened.readRegion(region))

  result.block match
    case PrimitiveBlock.Int16(block) =>
      println(s"values = ${block.toArray.toVector}")
      println(s"bytes read = ${result.receipt.bytesRead}")
    case _ => throw IllegalStateException("expected int16 data")
```

</details>

`SyncZarrWriter` is create-only: it never replaces an existing object and writes
the primary metadata object last. A successful `WriteReceipt` identifies a
complete publication. Reads return the decoded block together with an
`ExecutionReceipt` containing actual requests and byte counters.

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
| Metadata | `ZarrMetadata`, `ArrayDescriptor` | Parse and compile supported metadata into typed descriptors; unsupported required metadata returns `ZarrError`. |
| Reading | `SyncZarr`, `AsyncZarr` | Open arrays and groups, read regions or points, and materialize or fold selected fragments. |
| Writing | `SyncZarrWriter`, `AsyncZarrWriter` | Create arrays and groups without overwrite, resize, append, or concurrent mutation. |
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
  boldShape <- Shape(1200L, 100L)
  time <- AxisSelector.slice(0L, 1200L, step = 2L)
  voxels <- AxisSelector.indices(3L, 11L, 42L, 11L)
  selection <- FactoredSelection(boldShape, time, voxels)
yield selection
```

</details>

`selected` is an `Either[ZarrError, FactoredSelection]`. Pass the successful
selection to `OpenedArray.read`; the duplicate voxel index `11` is preserved.
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
