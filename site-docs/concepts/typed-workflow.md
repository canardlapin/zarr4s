# The typed workflow

The typed façade separates four concerns that are easy to conflate in a Zarr
application:

| Value | Role |
| --- | --- |
| `ArraySpec[D]` | Checked intent: dtype, logical shape, chunk shape, format, and optional metadata. |
| `DenseArray[D]` | Owned primitive values with the same dtype and shape as the specification. |
| `TypedOpenedArray[D]` | A dynamic descriptor refined against a compile-time dtype witness. |
| `TypedReadResult[D]` | Owned read values paired with the actual `ExecutionReceipt`. |

The type parameter is not a replacement for runtime validation. Zarr metadata
arrives dynamically, so opening first validates the descriptor and then
`asTyped` or `openTypedArray` checks that the requested `DType` matches what was
found. A mismatch returns `ZarrError.DTypeMismatch`; it is not silently cast.

## Construction and ownership

`ArraySpec` checks the array and chunk shapes before a writer visits the store.
`DenseArray.copyOf` checks the element count and makes an owned copy. This is
the normal boundary for values held in an ordinary Scala array:

```scala mdoc:compile-only
import zarr4s.*

val shape: Either[ZarrError, Shape] = Shape(4L, 4L)
val chunks: Either[ZarrError, Shape] = Shape(2L, 2L)
val spec: Either[ZarrError, ArraySpec[DType.Int32.type]] =
  for
    foundShape <- shape
    foundChunks <- chunks
    foundSpec <- ArraySpec(DType.Int32, foundShape, foundChunks)
  yield foundSpec

val owned: Either[ZarrError, DenseArray[DType.Int32.type]] =
  for
    foundShape <- shape
    found <- DenseArray.copyOf(
      DType.Int32,
      foundShape,
      Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    )
  yield found
```

Use `DenseArray.adopt` only when the caller deliberately transfers ownership
of the supplied primitive array and will never mutate it afterward. The
low-level `ChunkProvider` route is for callers who cannot materialize the
whole array, not a shorter spelling of the ordinary path.

## Create-only writes and reads

`SyncZarr.createArray` and `SyncZarr.createAndOpenArray` do not overwrite an
existing target. A successful write returns a `WriteOutcome.Complete` with a
`WriteReceipt`; an incomplete write retains progress and its typed error. A
store capability may reject an existing object, but the core does not hide a
delete-and-retry policy.

After a successful create, `readAll`, `readRegion`, `readPoints`, and
`read(selection)` all return typed results. Each result owns its values and
retains the receipt from the dynamic reader, so a caller can inspect I/O
accounting without coupling application logic to internal chunk loops.

## Platform boundary

The shared `SyncZarr` and `AsyncZarr` APIs take caller-supplied store,
scheduling, limit, cache, retry, and codec capabilities. JVM filesystem and
HTTP stores live in the JVM projection; browser Fetch and browser codec
boundaries live in the Scala.js projection. The core does not select credentials
or global concurrency policy for the application.
