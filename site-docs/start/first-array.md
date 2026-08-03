# Create and read your first array

The shortest complete workflow uses an in-memory store. It has no filesystem,
network, or browser setup, so every line concerns the array itself.

The example creates a 4 × 6 matrix. Rows represent four observations and
columns represent six measures. Storage uses 2 × 3 chunks, but the read returns
one logical 4 × 6 value.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value match
    case Right(result) => result
    case Left(error)   => throw IllegalArgumentException(error.message)

val measurementShape = checked(Shape(4L, 6L))
val measurementChunks = checked(Shape(2L, 3L))
val measurementSpec = checked(
  ArraySpec(DType.Int16, measurementShape, measurementChunks)
    .flatMap(_.withDimensionNames(Vector(Some("observation"), Some("measure"))))
)

val measurementValues = checked(
  DenseArray.copyOf(
    DType.Int16,
    measurementShape,
    (1 to 24).map(_.toShort).toArray
  )
)

val measurementStore = checked(MemoryStore.empty)
val created = checked(
  SyncZarr.createAndOpenArray(measurementStore, measurementSpec, measurementValues)
)
val measurements = checked(created.opened)
val complete = checked(measurements.readAll())
```

Evaluate the facts a caller usually needs first:

```scala mdoc
(
  complete.shape.toVector,
  complete.data.toArray.take(6).toVector,
  complete.receipt.touchedChunks,
  complete.receipt.bytesRead
)
```

The array has 24 two-byte values, so this uncompressed full read transfers 48
data bytes. Four chunks are touched because the logical 4 × 6 array is stored
as a 2 × 2 grid of 2 × 3 chunks.

The `checked` helper belongs to this executable example. It makes mdoc stop at
the failing line. Application code should normally keep
`Either[ZarrError, A]`, match on the error, or translate it at an application
boundary.

| Operation | Values | Shape and dtype | Store | Result |
| --- | --- | --- | --- | --- |
| `DenseArray.copyOf` | Copies the supplied Scala array. | Checks element count; preserves `DType.Int16`. | Unchanged. | Owned `DenseArray`. |
| `createAndOpenArray` | Writes all logical values. | Writes the checked specification. | Creates new objects; never overwrites existing ones. | Write outcome plus a typed opened handle. |
| `readAll` | Materializes values in logical order. | Preserves full shape and dtype. | Reads metadata and touched chunks. | Owned values plus `ExecutionReceipt`. |

Next: [understand arrays, chunks, and typed values](../concepts/array-model.md).
