# Read a region

Use `Region.within` when the application needs a bounded rectangular part of
an opened array. It checks rank and bounds before the reader touches the
store. The returned `TypedReadResult` keeps both the selected shape and the
read receipt.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value match
    case Right(result) => result
    case Left(error)   => throw IllegalArgumentException(error.message)

val shape = checked(Shape(4L, 4L))
val chunks = checked(Shape(2L, 2L))
val spec = checked(ArraySpec(DType.Int32, shape, chunks))
val values = checked(
  DenseArray.copyOf(
    DType.Int32,
    shape,
    Array(
      0, 1, 2, 3,
      4, 5, 6, 7,
      8, 9, 10, 11,
      12, 13, 14, 15
    )
  )
)
val store = checked(MemoryStore.empty)
val created = checked(SyncZarr.createAndOpenArray(store, spec, values))
val opened = checked(created.opened)

val origin = checked(Coordinate(1L, 1L))
val extent = checked(Shape(2L, 2L))
val region = checked(Region.within(shape, origin, extent))
val selected = checked(opened.readRegion(region))
```

```scala mdoc
(selected.shape.toVector, selected.data.toArray.toVector)
```

```text
(Vector(2, 2), Vector(5, 6, 9, 10))
```

The values are returned in row-major logical order for this direct chunked
array. `Region.within` is still useful when the region is assembled from
runtime input: an invalid origin, extent, or rank becomes an `Either` value
before any object request occurs.

For non-rectangular selections, use `CoordinateBatch` and `readPoints`, or
build a `FactoredSelection` and call `read`. Those APIs preserve the same
typed result and receipt boundary; choose the representation that matches the
caller’s selection rather than converting everything to a dense mask.
