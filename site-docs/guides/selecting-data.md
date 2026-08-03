# Select data

Choose a selection by the shape of the question. Use a `Region` for one
rectangular block, `CoordinateBatch` for unrelated points, and
`FactoredSelection` for independent slice or gather rules on each axis.

The examples continue with the 4 × 6 measurement array:

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)

val measurementShape = checked(Shape(4L, 6L))
val measurementChunks = checked(Shape(2L, 3L))
val measurementSpec = checked(ArraySpec(DType.Int16, measurementShape, measurementChunks))
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
```

## Read one rectangular region

`Region.within` takes an origin and extent. Both have the array's rank. Bounds
are checked before the reader touches data objects.

```scala mdoc:silent
val region = checked(
  Region.within(
    measurementShape,
    checked(Coordinate(1L, 1L)),
    checked(Shape(2L, 3L))
  )
)
val rectangular = checked(measurements.readRegion(region))
```

```scala mdoc
(rectangular.shape.toVector, rectangular.data.toArray.toVector)
```

## Read unrelated points

`CoordinateBatch` preserves point order and duplicates. The result is a
one-dimensional array with one value per requested coordinate.

```scala mdoc:silent
val points = checked(
  CoordinateBatch.within(
    measurementShape,
    Seq(
      checked(Coordinate(3L, 5L)),
      checked(Coordinate(0L, 0L)),
      checked(Coordinate(3L, 5L))
    )
  )
)
val gatheredPoints = checked(measurements.readPoints(points))
```

```scala mdoc
(gatheredPoints.shape.toVector, gatheredPoints.data.toArray.toVector)
```

## Slice and gather by axis

`FactoredSelection` applies one selector per axis with Cartesian, or
orthogonal, semantics. Here the row order is `3, 1, 1`, and every second
column is selected. The repeated row remains repeated.

```scala mdoc:silent
val factored = checked(
  for
    rows <- AxisSelector.indices(3L, 1L, 1L)
    columns <- AxisSelector.slice(0L, 6L, step = 2L)
    selection <- FactoredSelection(measurementShape, rows, columns)
  yield selection
)
val selected = checked(measurements.read(factored))
```

```scala mdoc
(selected.shape.toVector, selected.data.toArray.toVector)
```

Descending slices are not supported. Use `AxisSelector.indices` with an
explicit descending sequence when order matters.

| Operation | Values | Output shape | Order and duplicates | Metadata and dtype |
| --- | --- | --- | --- | --- |
| `readRegion` | One bounded rectangle. | The region extent. | Row-major logical order. | Dtype preserved; descriptor unchanged. |
| `readPoints` | One value per coordinate. | `Shape(pointCount)`. | Request order and duplicates preserved. | Dtype preserved; descriptor unchanged. |
| `read(FactoredSelection)` | Cartesian product of axis selectors. | One length per selected axis. | Each axis preserves requested order and duplicates. | Dtype preserved; descriptor unchanged. |

Every result contains a fresh `ExecutionReceipt`. Compare
`requestedLogicalBytes` with `bytesRead` or use `readAmplification` when tuning
chunks, sharding, or remote access.

Next: [write arrays without hiding partial progress](writing-arrays.md).
