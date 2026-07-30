package zarr4s

class GeometrySuite extends munit.FunSuite:
  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("shape supports scalar, empty, and arbitrary-rank arrays"):
    assertEquals(value(Shape()).elementCount, Right(1L))
    assertEquals(value(Shape(2L, 0L, Long.MaxValue)).elementCount, Right(0L))
    assertEquals(value(Shape(2L, 3L, 4L, 5L, 6L)).rank.toInt, 5)

  test("shape rejects negative dimensions and detects overflow"):
    assert(Shape(2L, -1L).isLeft)
    assertEquals(
      value(Shape(Long.MaxValue, 2L)).elementCount,
      Left(ZarrError.ArithmeticOverflow("shape element count"))
    )

  test("region validates ranks, bounds, and empty boundary selections"):
    val shape = value(Shape(10L, 20L))
    val valid = Region.within(shape, value(Coordinate(8L, 20L)), value(Shape(2L, 0L)))
    assert(value(valid).isEmpty)
    assert(Region.within(shape, value(Coordinate(9L, 0L)), value(Shape(2L, 1L))).isLeft)
    assert(Region.within(shape, value(Coordinate(0L)), value(Shape(1L))).isLeft)

  test("coordinate batch preserves order and duplicates"):
    val shape = value(Shape(4L, 5L))
    val coordinates = Seq(
      value(Coordinate(3L, 4L)),
      value(Coordinate(0L, 1L)),
      value(Coordinate(3L, 4L))
    )
    val batch = value(CoordinateBatch.within(shape, coordinates))
    assertEquals(batch.count, 3)
    assertEquals(batch.coordinate(0), coordinates(0))
    assertEquals(batch.coordinate(1), coordinates(1))
    assertEquals(batch.coordinate(2), coordinates(2))

  test("regular grid handles scalar and zero-sized arrays"):
    val scalar = value(RegularGrid(value(Shape()), value(Shape())))
    assertEquals(scalar.gridShape, value(Shape()))

    val empty = value(RegularGrid(value(Shape(0L, 10L)), value(Shape(4L, 3L))))
    assertEquals(empty.gridShape, value(Shape(0L, 4L)))

  test("default chunk keys follow the v3 scalar and separator rules"):
    val scalar = ChunkCoordinate.unsafe(Array.emptyLongArray)
    val coordinate = ChunkCoordinate.unsafe(Array(2L, 3L, 4L))
    assertEquals(DefaultChunkKeyEncoding(ChunkSeparator.Slash).encode(scalar).value, "c")
    assertEquals(DefaultChunkKeyEncoding(ChunkSeparator.Slash).encode(coordinate).value, "c/2/3/4")
    assertEquals(DefaultChunkKeyEncoding(ChunkSeparator.Dot).encode(coordinate).value, "c.2.3.4")

  test("v2 chunk keys follow the normative scalar and separator rules"):
    val scalar = ChunkCoordinate.unsafe(Array.emptyLongArray)
    val coordinate = ChunkCoordinate.unsafe(Array(2L, 3L, 4L))
    assertEquals(V2ChunkKeyEncoding(ChunkSeparator.Dot).encode(scalar).value, "0")
    assertEquals(V2ChunkKeyEncoding(ChunkSeparator.Dot).encode(coordinate).value, "2.3.4")
    assertEquals(V2ChunkKeyEncoding(ChunkSeparator.Slash).encode(coordinate).value, "2/3/4")
