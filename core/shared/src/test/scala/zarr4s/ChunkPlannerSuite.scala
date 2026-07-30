package zarr4s

class ChunkPlannerSuite extends munit.FunSuite:
  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("region planner covers a request exactly in deterministic C order"):
    val shape = value(Shape(8L, 10L))
    val grid = value(RegularGrid(shape, value(Shape(4L, 4L))))
    val region = value(Region.within(shape, value(Coordinate(2L, 3L)), value(Shape(5L, 6L))))
    val plan = value(ChunkPlanner.planRegion(grid, region))

    assertEquals(
      plan.demands.map(_.coordinate.toVector),
      Vector(
        Vector(0L, 0L),
        Vector(0L, 1L),
        Vector(0L, 2L),
        Vector(1L, 0L),
        Vector(1L, 1L),
        Vector(1L, 2L)
      )
    )
    assertEquals(plan.stats, ReadPlanStats(6, 30L))

    val copied = plan.demands.map:
      case ChunkDemand(_, ChunkCopy.RegionCopy(copy)) => value(copy.extent.elementCount)
      case _                                          => fail("expected region copies")
    assertEquals(copied.sum, 30L)

  test("empty region produces no store reads"):
    val shape = value(Shape(8L, 10L))
    val grid = value(RegularGrid(shape, value(Shape(4L, 4L))))
    val region = value(Region.within(shape, value(Coordinate(8L, 0L)), value(Shape(0L, 5L))))
    assertEquals(
      value(ChunkPlanner.planRegion(grid, region)),
      ReadPlan(Vector.empty, ReadPlanStats(0, 0L))
    )

  test("scalar region maps to the single scalar chunk"):
    val shape = value(Shape())
    val grid = value(RegularGrid(shape, value(Shape())))
    val region = value(Region.within(shape, value(Coordinate()), value(Shape())))
    val plan = value(ChunkPlanner.planRegion(grid, region))
    assertEquals(plan.demands.map(_.coordinate.toVector), Vector(Vector.empty))
    assertEquals(plan.stats, ReadPlanStats(1, 1L))

  test("point planner groups chunks while preserving result order and duplicates"):
    val shape = value(Shape(8L, 8L))
    val grid = value(RegularGrid(shape, value(Shape(4L, 4L))))
    val points = value(
      CoordinateBatch.within(
        shape,
        Seq(
          value(Coordinate(6L, 1L)),
          value(Coordinate(1L, 2L)),
          value(Coordinate(6L, 1L)),
          value(Coordinate(2L, 7L))
        )
      )
    )
    val plan = value(ChunkPlanner.planPoints(grid, points))

    assertEquals(
      plan.demands.map(_.coordinate.toVector),
      Vector(
        Vector(0L, 0L),
        Vector(0L, 1L),
        Vector(1L, 0L)
      )
    )
    val gathers = plan.demands.map:
      case ChunkDemand(_, ChunkCopy.GatherCopy(offsets)) =>
        offsets.source.toVector -> offsets.destination.toVector
      case _ => fail("expected gather copies")
    assertEquals(
      gathers,
      Vector(
        Vector(6) -> Vector(1),
        Vector(11) -> Vector(3),
        Vector(9, 9) -> Vector(0, 2)
      )
    )

  test("planner enforces an explicit chunk budget"):
    val shape = value(Shape(100L, 100L))
    val grid = value(RegularGrid(shape, value(Shape(10L, 10L))))
    val region = value(Region.within(shape, value(Coordinate(0L, 0L)), shape))
    assert(ChunkPlanner.planRegion(grid, region, PlanningLimits(maxChunks = 10)).isLeft)

  test("point offsets use the nominal edge-chunk strides"):
    val shape = value(Shape(6L, 6L))
    val grid = value(RegularGrid(shape, value(Shape(4L, 4L))))
    val points = value(CoordinateBatch.within(shape, Seq(value(Coordinate(5L, 5L)))))
    val plan = value(ChunkPlanner.planPoints(grid, points))
    plan.demands.head.copy match
      case ChunkCopy.GatherCopy(offsets) => assertEquals(offsets.source.toVector, Vector(5))
      case _                             => fail("expected gather copy")

  test("rank-five planning is a normal kernel operation"):
    val shape = value(Shape(2L, 3L, 4L, 5L, 6L))
    val grid = value(RegularGrid(shape, value(Shape(1L, 2L, 2L, 3L, 4L))))
    val region = value(
      Region.within(
        shape,
        value(Coordinate(0L, 1L, 1L, 1L, 1L)),
        value(Shape(2L, 2L, 3L, 4L, 5L))
      )
    )
    val plan = value(ChunkPlanner.planRegion(grid, region))
    assertEquals(plan.stats.requestedElements, 240L)
    assertEquals(plan.demands.map(_.coordinate.toVector).distinct.size, plan.stats.chunkCount)
