# Z11 Ravel interoperability contract

Status: compile-checked implementation contract. Publication remains disabled until Ravel has an
immutable released artifact.

## Boundary

`zarr4s-core` continues to own Zarr metadata, stores, chunk planning, codecs, errors, receipts, and
create-only publication. `ravel-core` owns dense storage, layout, views, and computation. The
optional `zarr4s-interop-ravel` cross-project owns only exact dtype correspondence, checked shape
conversion, owned NDArray materialization, and a Ravel-backed `ChunkProvider`.

`DenseArray` remains the dependency-free zarr4s transfer value. The adapter does not introduce a
second descriptor, codec, or I/O model. ScalaFIM is a later downstream acceptance consumer and does
not determine this API.

The existing public seams are sufficient:

- `OpenedArray` and `AsyncOpenedArray` return `ReadResult(block, shape, receipt)` for complete,
  region, point, and factored selections.
- `TypedChunkProvider` and the sync/async create facades already preserve typed descriptors,
  create-only outcomes, and explicit runtimes and limits.

No core hook or Ravel import in `zarr4s-core` is required.

## Public contract

The exact element relation is represented by `RavelElement[D]` and the match type
`RavelValue[D]`. Instances exist only for:

| Zarr dtype | Ravel element |
| --- | --- |
| `Bool` | `Boolean` |
| `Int8` | `Byte` |
| `UInt8` | `ravel.UInt8` |
| `Int16` | `Short` |
| `UInt16` | `ravel.UInt16` |
| `Int32` | `Int` |
| `Int64` | `Long` |
| `Float32` | `Float` |
| `Float64` | `Double` |

There are deliberately no exact instances for Float16, UInt32, UInt64, complex, raw, or custom
dtypes. A future converting API must have a different name and explicit conversion policy.

Typed sync handles provide `readAllNDArray`, `readRegionNDArray`, `readPointsNDArray`, and
`readNDArray`. Async handles provide the same parameter and result contracts under names ending in
`Async`; using identical top-level extension names produces conflicting Scala default-argument
methods. Both paths retain the original `ExecutionReceipt`.

`RavelArraySource.fromCanonical(dtype, array)` refines an immutable, whole-buffer, canonical Ravel
array without copying its payload. `RavelArraySource.copyOf(dtype, view)` explicitly materializes a
view in logical C-order. Borrowed and mutable arrays are not accepted by either signature.

`RavelZarr` and `AsyncRavelZarr` layer over the existing descriptor compiler, writer, runtime,
limits, progress, and open-after-create logic. Canonical sources are read linearly into nominal
chunk blocks; they are never copied into a complete intermediate Zarr dense value.

## Shape and error rules

Zarr dimensions are `Long`; Ravel dimensions and total buffers are bounded by `Int`. Every axis is
checked before narrowing, including dimensions of a zero-sized array. A product above
`Int.MaxValue` fails separately. Adapter failures use `RavelInteropError`; native Zarr failures are
retained as `RavelInteropError.Zarr(error)` rather than copied into a parallel error hierarchy.

## Ownership and allocation

Reads allocate the owned Ravel destination directly with `NDArray.build` and populate it from the
decoded `PrimitiveBlock`; they do not construct `DenseArray`. Writes allocate only the nominal
filled chunk blocks required by the existing codec pipeline. These are structural contracts, not a
claim of storage-level zero-copy across codecs or platforms.

## Compatibility and release

The compile spike uses Scala 3.7.4 and the local `ravel-core` 1.0.0-SNAPSHOT built from revision
`d0f7bacfe3b750519dc49aca8fd466ef70ef24ec`. Both libraries are Apache-2.0 and cross-build for JVM
and Scala.js. `zarr4s-interop-ravel / publish / skip` stays true until an immutable Ravel artifact
exists; source or local snapshot success is not release evidence.

Rejected alternatives:

- adding Ravel to `zarr4s-core`, which would violate the core dependency boundary;
- replacing `DenseArray`, which would force a computational library on ordinary Zarr users;
- routing reads through `DenseArray`, which adds an avoidable output-sized copy;
- accepting arbitrary views while copying silently;
- widening unsupported dtypes under the native read names;
- exposing Ravel platform storage or borrowing decoded chunk buffers.
