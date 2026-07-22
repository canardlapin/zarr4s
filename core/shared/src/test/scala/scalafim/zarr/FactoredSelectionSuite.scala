package scalafim.zarr

class FactoredSelectionSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def slice(start: Long, stop: Long, step: Long = 1L): AxisSelector =
    AxisSelector.Slice(zvalue(AxisSlice(start, stop, step)))

  private def indices(values: Long*): AxisSelector =
    AxisSelector.Indices(zvalue(AxisIndices.from(values)))

  private def expanded(plan: ReadPlan, grid: RegularGrid): Vector[(Vector[Long], Vector[Long])] =
    val result = Vector.newBuilder[(Vector[Long], Vector[Long])]
    plan.demands.foreach: demand =>
      demand.copy match
        case ChunkCopy.FactoredCopy(copy) =>
          val cursor = new Array[Long](copy.axes.length)
          var element = 0L
          while element < copy.elementCount do
            val source = new Array[Long](copy.axes.length)
            val destination = new Array[Long](copy.axes.length)
            var axis = 0
            while axis < copy.axes.length do
              source(axis) = demand.coordinate.axis(axis) * grid.chunkShape.axis(axis) +
                copy.axes(axis).source(cursor(axis))
              destination(axis) = copy.axes(axis).destination(cursor(axis))
              axis += 1
            result += source.toVector -> destination.toVector
            element += 1L
            advance(cursor, copy.fragmentShape)
        case _ => fail("expected factored copy")
    result.result()

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.axis(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  test("factored slices and gathers preserve Cartesian output order and duplicates"):
    val shape = zvalue(Shape(4L, 5L))
    val grid = zvalue(RegularGrid(shape, zvalue(Shape(2L, 3L))))
    val selection = zvalue(FactoredSelection.within(shape, Vector(
      slice(0L, 4L, 2L),
      indices(4L, 1L, 4L)
    )))
    val plan = zvalue(ChunkPlanner.planFactored(grid, selection))
    assertEquals(selection.outputShape.toVector, Vector(2L, 3L))
    assertEquals(plan.stats, ReadPlanStats(4, 6L))
    assertEquals(plan.demands.map(_.coordinate.toVector), Vector(
      Vector(0L, 0L),
      Vector(0L, 1L),
      Vector(1L, 0L),
      Vector(1L, 1L)
    ))
    val ordered = expanded(plan, grid).sortBy: (_, destination) =>
      destination(0) * 3L + destination(1)
    assertEquals(ordered, Vector(
      Vector(0L, 4L) -> Vector(0L, 0L),
      Vector(0L, 1L) -> Vector(0L, 1L),
      Vector(0L, 4L) -> Vector(0L, 2L),
      Vector(2L, 4L) -> Vector(1L, 0L),
      Vector(2L, 1L) -> Vector(1L, 1L),
      Vector(2L, 4L) -> Vector(1L, 2L)
    ))

  test("slice planning skips untouched chunks without enumerating the index span"):
    val shape = zvalue(Shape(1000000000000L))
    val grid = zvalue(RegularGrid(shape, zvalue(Shape(16L))))
    val selection = zvalue(FactoredSelection.within(shape, Vector(
      slice(1L, 1000000000000L, 100000000000L)
    )))
    val plan = zvalue(ChunkPlanner.planFactored(grid, selection))
    assertEquals(selection.outputShape.toVector, Vector(10L))
    assertEquals(plan.demands.length, 10)
    assertEquals(
      expanded(plan, grid).map(_._1.head),
      Vector.tabulate(10)(index => 1L + index.toLong * 100000000000L)
    )

  test("scalar, empty, and rank-five factored selections use one planner"):
    val scalarShape = zvalue(Shape())
    val scalarGrid = zvalue(RegularGrid(scalarShape, zvalue(Shape())))
    val scalarPlan = zvalue(ChunkPlanner.planFactored(
      scalarGrid,
      FactoredSelection.all(scalarShape)
    ))
    assertEquals(scalarPlan.demands.length, 1)
    assertEquals(scalarPlan.stats.requestedElements, 1L)

    val emptyShape = zvalue(Shape(3L, 0L, 4L))
    val emptyGrid = zvalue(RegularGrid(emptyShape, zvalue(Shape(2L, 2L, 2L))))
    val emptyPlan = zvalue(ChunkPlanner.planFactored(
      emptyGrid,
      FactoredSelection.all(emptyShape)
    ))
    assertEquals(emptyPlan.demands, Vector.empty)
    assertEquals(emptyPlan.stats.requestedElements, 0L)

    val rankFiveShape = zvalue(Shape(2L, 3L, 4L, 5L, 6L))
    val rankFiveGrid = zvalue(RegularGrid(rankFiveShape, zvalue(Shape(1L, 2L, 3L, 4L, 5L))))
    val rankFive = zvalue(FactoredSelection.within(rankFiveShape, Vector(
      AxisSelector.All,
      slice(1L, 3L),
      indices(3L, 0L),
      slice(0L, 5L, 2L),
      indices(5L, 1L, 5L)
    )))
    val rankFivePlan = zvalue(ChunkPlanner.planFactored(rankFiveGrid, rankFive))
    assertEquals(rankFive.outputShape.toVector, Vector(2L, 2L, 2L, 3L, 3L))
    assertEquals(rankFivePlan.stats.requestedElements, 72L)
    assertEquals(expanded(rankFivePlan, rankFiveGrid).length, 72)

  test("fMRI-shaped time by voxel gather shares axis factors across the chunk product"):
    val shape = zvalue(Shape(1200L, 200000L))
    val grid = zvalue(RegularGrid(shape, zvalue(Shape(64L, 4096L))))
    val masked = AxisIndices.from(0L until 200000L by 2L).fold(
      error => fail(error.message),
      identity
    )
    val selection = zvalue(FactoredSelection.within(shape, Vector(
      AxisSelector.All,
      AxisSelector.Indices(masked)
    )))
    val plan = zvalue(ChunkPlanner.planFactored(grid, selection))
    assertEquals(selection.outputShape.toVector, Vector(1200L, 100000L))
    assertEquals(selection.requestedElements, 120000000L)
    assertEquals(plan.demands.length, 19 * 49)
    val footprint = plan.factored.getOrElse(fail("missing factored planner receipt"))
    assertEquals(footprint.axisGroups, 68)
    assertEquals(footprint.explicitIndexEntries, 100000L)
    assertEquals(footprint.demandAxisReferences, 1862L)
    assert(footprint.structuralEntries * 1000L < selection.requestedElements)
    val first = plan.demands.head.copy match
      case ChunkCopy.FactoredCopy(copy) => copy
      case _ => fail("expected factored copy")
    val nextVoxelChunk = plan.demands(1).copy match
      case ChunkCopy.FactoredCopy(copy) => copy
      case _ => fail("expected factored copy")
    val nextTimeChunk = plan.demands(49).copy match
      case ChunkCopy.FactoredCopy(copy) => copy
      case _ => fail("expected factored copy")
    assert(first.axes(0).asInstanceOf[AnyRef] eq nextVoxelChunk.axes(0).asInstanceOf[AnyRef])
    assert(first.axes(1).asInstanceOf[AnyRef] eq nextTimeChunk.axes(1).asInstanceOf[AnyRef])

  test("factored construction and planning reject invalid bounds and explicit limits"):
    assert(AxisSlice(3L, 1L).isLeft)
    assert(AxisSlice(1L, 3L, 0L).isLeft)
    assert(AxisSlice(3L, 1L, -1L).isLeft)
    assert(AxisIndices.from(Vector(0L, -1L)).isLeft)

    val shape = zvalue(Shape(4L, 5L))
    assert(FactoredSelection.within(shape, Vector(AxisSelector.All)).isLeft)
    assert(FactoredSelection.within(shape, Vector(
      AxisSelector.All,
      indices(5L)
    )).isLeft)

    val grid = zvalue(RegularGrid(shape, zvalue(Shape(2L, 3L))))
    val selection = zvalue(FactoredSelection.within(shape, Vector(
      AxisSelector.All,
      indices(4L, 1L, 4L)
    )))
    assert(ChunkPlanner.planFactored(grid, selection, PlanningLimits(maxChunks = 3)).isLeft)
    assert(ChunkPlanner.planFactored(
      grid,
      selection,
      PlanningLimits(maxAxisIndexEntries = 2)
    ).isLeft)
