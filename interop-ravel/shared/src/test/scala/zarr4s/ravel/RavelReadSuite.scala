package zarr4s.ravel

import _root_.zarr4s.*
import scala.concurrent.ExecutionContext

class RavelReadSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def intSpec(
      shape: Shape,
      chunks: Shape,
      fill: Option[Int] = None
  ): ArraySpec[DType.Int32.type] =
    zvalue(ArraySpec.withOptions(DType.Int32, shape, chunks, fill, None))

  private def writeDirect(
      shape: Shape,
      chunks: Shape,
      values: Array[Int]
  ): MemoryStore =
    val store = zvalue(MemoryStore.empty)
    val data = zvalue(DenseArray.copyOf(DType.Int32, shape, values))
    val result = zvalue(SyncZarr.createArray(store, intSpec(shape, chunks), data))
    result.outcome.toEither.left.foreach(error => fail(error.message))
    store

  test("full Ravel read preserves the dynamic reader's shape values and receipt"):
    val shape = zvalue(Shape(4L, 5L))
    val store = writeDirect(shape, zvalue(Shape(2L, 3L)), (0 until 20).toArray)
    val opened = zvalue(SyncZarr.openTypedArray(store, DType.Int32))
    val dynamic = zvalue(opened.asOpenedArray.readAll())
    val result = rvalue(opened.readAllNDArray())

    assertEquals(result.data.elementsIterator.toVector, (0 until 20).toVector)
    assertEquals(result.data.shape.toString, "(4, 5)")
    assertEquals(result.receipt, dynamic.receipt)

  test("region point and factored reads preserve logical order and selection shape"):
    val shape = zvalue(Shape(4L, 5L))
    val store = writeDirect(shape, zvalue(Shape(2L, 3L)), (0 until 20).toArray)
    val opened = zvalue(SyncZarr.openTypedArray(store, DType.Int32))

    val region = zvalue(
      Region.within(shape, zvalue(Coordinate(1L, 1L)), zvalue(Shape(2L, 3L)))
    )
    val regionResult = rvalue(opened.readRegionNDArray(region))
    assertEquals(regionResult.data.elementsIterator.toVector, Vector(6, 7, 8, 11, 12, 13))
    assertEquals(regionResult.data.shape.toString, "(2, 3)")
    assert(regionResult.receipt.touchedChunks > 0)

    val points = zvalue(
      CoordinateBatch.within(
        shape,
        Seq(zvalue(Coordinate(3L, 4L)), zvalue(Coordinate(0L, 0L)), zvalue(Coordinate(1L, 2L)))
      )
    )
    val pointResult = rvalue(opened.readPointsNDArray(points))
    assertEquals(pointResult.data.elementsIterator.toVector, Vector(19, 0, 7))
    assertEquals(pointResult.data.shape.toString, "(3)")

    val factored = zvalue(
      FactoredSelection.within(
        shape,
        Vector(
          AxisSelector.Indices(zvalue(AxisIndices.from(Vector(3L, 1L, 3L)))),
          AxisSelector.Slice(zvalue(AxisSlice(0L, 5L, 2L)))
        )
      )
    )
    val factoredResult = rvalue(opened.readNDArray(factored))
    assertEquals(
      factoredResult.data.elementsIterator.toVector,
      Vector(15, 17, 19, 5, 7, 9, 15, 17, 19)
    )
    assertEquals(factoredResult.data.shape.toString, "(3, 3)")

  test("sharded reads use the same direct NDArray materialization"):
    val shape = zvalue(Shape(4L, 4L))
    val outer = zvalue(Shape(4L, 4L))
    val inner = zvalue(Shape(2L, 2L))
    val spec = intSpec(shape, outer)
    val data = zvalue(DenseArray.copyOf(DType.Int32, shape, (1 to 16).toArray))
    val descriptor = zvalue(ArrayDescriptor.sharded(spec, ShardingSpec.indexed(inner)))
    val provider = zvalue(ChunkProvider.fromDense(descriptor, data))
    val store = zvalue(MemoryStore.empty)
    SyncZarrWriter
      .create(store, descriptor, provider)
      .toEither
      .left
      .foreach(error => fail(error.message))

    val result = rvalue(zvalue(SyncZarr.openTypedArray(store, DType.Int32)).readAllNDArray())
    assertEquals(result.data.elementsIterator.toVector, (1 to 16).toVector)
    assert(result.receipt.rangeRequests > 0)

  test("missing chunks synthesize typed fill directly in the Ravel destination"):
    val shape = zvalue(Shape(3L, 4L))
    val store = zvalue(MemoryStore.empty)
    val filled = intSpec(shape, zvalue(Shape(2L, 3L)), Some(-9))
    val write = zvalue(SyncZarr.createFillArray(store, filled))
    assert(write.outcome.toEither.isRight)

    val result = rvalue(zvalue(SyncZarr.openTypedArray(store, DType.Int32)).readAllNDArray())
    assertEquals(result.data.elementsIterator.toVector, Vector.fill(12)(-9))
    assertEquals(result.receipt.objectRequests, 4)
    assertEquals(result.receipt.bytesRead, 0L)

  test("native read failures remain typed Zarr failures at the adapter boundary"):
    val shape = zvalue(Shape(2L, 2L))
    val original = writeDirect(shape, shape, Array(1, 2, 3, 4)).snapshot
    val chunkKey = original.keys.find(_.startsWith("c/")).getOrElse(fail("missing chunk"))
    val truncated = original(chunkKey).slice(0, original(chunkKey).length - 1)
    val store = zvalue(MemoryStore(original.updated(chunkKey, truncated)))
    val opened = zvalue(SyncZarr.openTypedArray(store, DType.Int32))

    opened.readAllNDArray() match
      case Left(RavelInteropError.Zarr(_)) => ()
      case other => fail(s"expected wrapped Zarr read failure, found $other")

  test("async region point and factored paths preserve sync values"):
    val shape = zvalue(Shape(4L, 5L))
    val syncStore = writeDirect(shape, zvalue(Shape(2L, 3L)), (0 until 20).toArray)
    val store = zvalue(AsyncMemoryStore(syncStore.snapshot))
    val region = zvalue(
      Region.within(shape, zvalue(Coordinate(1L, 1L)), zvalue(Shape(2L, 3L)))
    )
    val points = zvalue(
      CoordinateBatch.within(
        shape,
        Seq(zvalue(Coordinate(3L, 4L)), zvalue(Coordinate(0L, 0L)))
      )
    )
    val factored = zvalue(
      FactoredSelection.within(
        shape,
        Vector(AxisSelector.Slice(zvalue(AxisSlice(0L, 4L, 2L))), AxisSelector.All)
      )
    )

    AsyncZarr
      .openTypedArray(store, DType.Int32)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          for
            regionResult <- opened.readRegionNDArrayAsync(region)
            pointResult <- opened.readPointsNDArrayAsync(points)
            factoredResult <- opened.readNDArrayAsync(factored)
          yield (regionResult, pointResult, factoredResult) match
            case (Right(foundRegion), Right(foundPoints), Right(foundFactored)) =>
              assertEquals(
                foundRegion.data.elementsIterator.toVector,
                Vector(6, 7, 8, 11, 12, 13)
              )
              assertEquals(foundPoints.data.elementsIterator.toVector, Vector(19, 0))
              assertEquals(
                foundFactored.data.elementsIterator.toVector,
                Vector(0, 1, 2, 3, 4, 10, 11, 12, 13, 14)
              )
            case other => fail(s"unexpected async selection failure: $other")
