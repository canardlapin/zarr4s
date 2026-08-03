# Write arrays safely

The writer creates new arrays and groups. It does not overwrite, append,
resize, delete, or make a generic object store transactional. This narrower
contract lets every write report exactly what was created.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)

val shape = checked(Shape(4L, 6L))
val chunks = checked(Shape(2L, 3L))
val spec = checked(ArraySpec(DType.Int16, shape, chunks))
val values = checked(
  DenseArray.copyOf(DType.Int16, shape, (1 to 24).map(_.toShort).toArray)
)
val store = checked(MemoryStore.empty)
val write = checked(SyncZarr.createArray(store, spec, values))
```

Handle the outcome explicitly:

```scala mdoc
write.outcome match
  case WriteOutcome.Complete(receipt) =>
    ("complete", receipt.totalObjects, receipt.visitedChunks, receipt.totalBytes.toLong)
  case WriteOutcome.Incomplete(progress, error) =>
    ("incomplete", progress.createdObjects, progress.visitedChunks, error.message)
```

For this direct, uncompressed array, four data objects and one metadata object
are created. The exact metadata byte count is an implementation result, not a
portable size guarantee.

## Treat an existing target as a conflict

Calling the writer again with the same store and path does not replace the
array:

```scala mdoc
val conflict = checked(SyncZarr.createArray(store, spec, values))

conflict.outcome match
  case WriteOutcome.Complete(_) => "unexpected overwrite"
  case WriteOutcome.Incomplete(progress, error) =>
    (progress.createdObjects, error.message)
```

The incomplete outcome retains progress even when no object was created. On a
store where a later object fails, `progress.objects` and
`progress.metadataObjects` identify objects that may remain.

## Create a fill-only array

`createFillArray` writes metadata and omits chunks whose logical values are all
the declared fill value:

```scala mdoc:silent
val fillStore = checked(MemoryStore.empty)
val fillSpec = spec.withFill(7.toShort)
val fillWrite = checked(SyncZarr.createFillArray(fillStore, fillSpec))
val filled = checked(SyncZarr.openTypedArray(fillStore, DType.Int16))
val fillRead = checked(filled.readAll())
```

```scala mdoc
(
  fillWrite.receipt.map(_.omittedFillChunks),
  fillRead.data.toArray.distinct.toVector,
  fillStore.snapshot.keySet.toVector.sorted
)
```

No physical data chunk is needed for the fill-only array.

## Choose v2 or v3 deliberately

`ArraySpec` defaults to Zarr v3. Use `spec.asFormat(ZarrFormat.V2)` when a
consumer requires v2. The v2 writer uses v2 metadata and chunk keys; it does
not emulate v3 sharding. The same typed values can be supplied to either
format.

## Stream values with a provider

`DenseArray` is intentionally bounded by a JVM/JavaScript array and therefore
by `Int.MaxValue` elements. For larger or generated data, implement
`ChunkProvider`, attach its promised dtype with `TypedChunkProvider.from`, and
call `createArrayFromProvider`. The provider receives one nominal stored chunk
shape at a time and must supply fill values for border overhang.

This section is unusually difficult to explain because the high-level write
call returns an outer `Either` for preparation and a nested `WriteOutcome` for
publication. The distinction is valid—preparation can fail before any write,
while publication can fail after partial progress—but routine callers need two
failure-handling layers. A simpler convenience API could return one completed
result and reserve the progress-rich result for an explicitly detailed writer
method.

| Write | Input values | Existing objects | Completion evidence |
| --- | --- | --- | --- |
| `createArray` | Dense values or a provider. | Never replaced. | `WriteOutcome`; complete receipt or incomplete progress. |
| `createFillArray` | Declared fill value. | Never replaced. | Receipt records omitted fill chunks. |
| `JvmZarr.createArray` | Dense values or provider. | Publishes to a new filesystem target. | Staged directory publication plus `WriteOutcome`. |

Next: [create and navigate a group](groups.md).
