# Use zarr4s with Ravel arrays

Add the optional Ravel adapter when a program should read Zarr data into an owned Ravel `NDArray`, compute with Ravel, and create another Zarr array without passing through `DenseArray`. The adapter is a separate artifact; `zarr4s-core` remains independent of Ravel.

The adapter is pre-release. Its coordinate is reserved as `zarr4s-interop-ravel`, but it is not published while Ravel has no immutable release. The repository's [standalone consumer](https://github.com/canardlapin/zarr4s/tree/main/examples/ravel-standalone-consumer) documents the commit-labelled local publication court. Do not use an unpinned `1.0.0-SNAPSHOT` as a release dependency.

## Read, transform, and create an array

The complete workflow uses only the typed zarr4s and Ravel APIs:

```scala mdoc:silent
import ravel.{NDArray, Shape as RavelShape}
import ravel.DType.given
import ravel.map
import zarr4s.*
import zarr4s.ravel.*

def requireZarr[A](result: Either[ZarrError, A]): A =
  result.fold(error => throw IllegalArgumentException(error.message), identity)

def requireRavel[A](result: Either[RavelInteropError, A]): A =
  result.fold(error => throw IllegalArgumentException(error.message), identity)

val shape = requireZarr(Shape(2L, 3L))
val chunks = requireZarr(Shape(1L, 3L))
val spec = requireZarr(ArraySpec(DType.Float32, shape, chunks))
val inputPath = requireZarr(ZarrPath("input"))
val outputPath = requireZarr(ZarrPath("output"))
val store = requireZarr(MemoryStore.empty)

val input = NDArray.fromSeq(
  RavelShape(2, 3),
  Seq(0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f)
)
val inputSource = requireRavel(
  RavelArraySource.fromCanonical(DType.Float32, input)
)
val created = requireRavel(
  RavelZarr.createAndOpenArray(store, spec, inputSource, path = inputPath)
)
val opened = requireZarr(created.opened)
val read = requireRavel(opened.readAllNDArray())

val transformed = read.data.map(value => value * 2.0f + 1.0f)
val outputSource = requireRavel(
  RavelArraySource.fromCanonical(DType.Float32, transformed)
)
val written = requireRavel(
  RavelZarr.createArray(store, spec, outputSource, path = outputPath)
)
```

Check publication separately from adapter preparation:

```scala mdoc:silent
val outputReceipt = written.outcome match
  case WriteOutcome.Complete(receipt) => receipt
  case WriteOutcome.Incomplete(progress, error) =>
    throw IllegalStateException(
      s"write stopped after ${progress.createdObjects} objects: ${error.message}"
    )

val output = requireZarr(
  SyncZarr.openTypedArray(store, DType.Float32, path = outputPath)
)
val verified = requireRavel(output.readAllNDArray())
```

```scala mdoc
(verified.data.elementsIterator.toVector, read.receipt.bytesRead, outputReceipt.totalObjects)
```

`read.data` owns one canonical Ravel output buffer. `read.receipt` is the zarr4s `ExecutionReceipt` for the underlying read. The output writer retains either a complete `WriteReceipt` or incomplete progress and the typed failure.

## Choose an exact dtype

The adapter accepts only representations that are exact in both libraries:

| Zarr dtype | Ravel element |
| --- | --- |
| `bool` | `Boolean` |
| `int8` | `Byte` |
| `uint8` | `UInt8` |
| `int16` | `Short` |
| `uint16` | `UInt16` |
| `int32` | `Int` |
| `int64` | `Long` |
| `float32` | `Float` |
| `float64` | `Double` |

`float16`, `uint32`, `uint64`, `complex64`, `complex128`, raw, structured, string, and object representations are not widened or reinterpreted. Static calls fail to compile when no `RavelElement[D]` exists; runtime refinement returns `RavelInteropError.UnsupportedDType`.

Zarr shapes use `Long` dimensions, while Ravel 1.0 uses `Int`. The adapter rejects any dimension or total element count that Ravel cannot represent before allocating the output.

## Make ownership explicit

`RavelArraySource.fromCanonical` accepts an immutable, whole-buffer, C-order Ravel array and retains its owner without copying the values. A slice, transpose, reverse, broadcast, or other view is not accepted by that constructor. Call `RavelArraySource.copyOf` when the write should materialize such a view in logical C-order.

| Operation | Result ownership | Whole-array copy |
| --- | --- | --- |
| `readAllNDArray` and selection variants | New owned canonical Ravel array | One Ravel output buffer after zarr4s decoding. |
| `fromCanonical` | Retains the immutable Ravel owner | No. |
| Canonical write chunk production | New nominal zarr4s chunk buffer | No source-array copy; storage is bounded by chunk size. |
| `copyOf` | New owned canonical Ravel array | Yes, explicitly. |

These are measured claims for the retained court, not a zero-copy claim. See the [Z11 allocation evidence](https://github.com/canardlapin/zarr4s/blob/main/docs/benchmarks/zarr-z11-ravel-allocation.md) for workload, allocation bytes, peak-memory qualifications, and raw receipts.

## Use the asynchronous forms

The asynchronous façade has the same dtype, shape, ownership, and outcome rules. Its names carry an `Async` suffix where Scala's top-level extension/default-argument rules would otherwise make the synchronous and asynchronous forms collide:

```scala mdoc:silent
import scala.concurrent.ExecutionContext.Implicits.global

val asyncStore = requireZarr(AsyncMemoryStore(Map.empty))
val asyncInput = requireRavel(
  RavelArraySource.fromCanonical(DType.Float32, input)
)
val asyncCreated = AsyncRavelZarr.createAndOpenArray(asyncStore, spec, asyncInput)

val asyncRead = asyncCreated.flatMap:
  case Left(error) => scala.concurrent.Future.successful(Left(error))
  case Right(result) => result.opened match
    case Left(error) =>
      scala.concurrent.Future.successful(Left(RavelInteropError.Zarr(error)))
    case Right(opened) => opened.readAllNDArrayAsync()
```

Use `readRegionNDArray`, `readPointsNDArray`, and `readNDArray` for synchronous selections. Their asynchronous counterparts are `readRegionNDArrayAsync`, `readPointsNDArrayAsync`, and `readNDArrayAsync`. All retain the original zarr4s execution receipt.

## Boundaries

The adapter does not add storage backends, retries, credentials, codecs, caching, overwrite, append, resize, deletion, labels, units, calibration, image meaning, or another array abstraction to Zarr metadata. Supply those policies through zarr4s or a downstream library. Optional codecs remain separate artifacts and runtimes.

The guide's mdoc workflow executes on the JVM. The same public adapter sources and semantic suite run on JVM and Node.js Scala.js; the standalone consumer compiles and runs both platform artifacts published to the local verification repository. This page does not claim Scala Native support or browser performance.

Next: [create and navigate a group](groups.md).
