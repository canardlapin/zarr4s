package scalafim.zarr

import scala.collection.mutable

enum ArraySelection:
  case RegionSelection(region: Region)
  case PointSelection(points: CoordinateBatch)
  case Factored(selection: FactoredSelection)

final case class CopyRegion(
    sourceOrigin: Coordinate,
    destinationOrigin: Coordinate,
    extent: Shape
)

final class IntOffsets private (private[zarr] val values: Array[Int]):
  def size: Int = values.length
  def apply(index: Int): Int = values(index)
  def toVector: Vector[Int] = values.toVector

  override def equals(other: Any): Boolean = other match
    case that: IntOffsets => java.util.Arrays.equals(values, that.values)
    case _ => false

  override def hashCode(): Int = java.util.Arrays.hashCode(values)

object IntOffsets:
  private[zarr] def unsafe(values: Array[Int]): IntOffsets =
    new IntOffsets(java.util.Arrays.copyOf(values, values.length))

final case class GatherOffsets(source: IntOffsets, destination: IntOffsets):
  require(source.size == destination.size, "source and destination gather offsets must align")

enum ChunkCopy:
  case RegionCopy(region: CopyRegion)
  case GatherCopy(offsets: GatherOffsets)
  private[zarr] case FactoredCopy(copy: FactoredChunkCopy)

final case class ChunkDemand(coordinate: ChunkCoordinate, copy: ChunkCopy)

final case class ReadPlanStats(chunkCount: Int, requestedElements: Long)

final case class FactoredPlanStats(
    axisGroups: Int,
    explicitIndexEntries: Long,
    demandAxisReferences: Long
):
  require(axisGroups >= 0, "axisGroups must be non-negative")
  require(explicitIndexEntries >= 0L, "explicitIndexEntries must be non-negative")
  require(demandAxisReferences >= 0L, "demandAxisReferences must be non-negative")

  val structuralEntries: Long =
    explicitIndexEntries + axisGroups.toLong + demandAxisReferences

final case class ReadPlan(
    demands: Vector[ChunkDemand],
    stats: ReadPlanStats,
    factored: Option[FactoredPlanStats] = None
)

final case class PlanningLimits(
    maxChunks: Int = 1000000,
    maxAxisIndexEntries: Int = 10000000
):
  require(maxChunks >= 0, "maxChunks must be non-negative")
  require(maxAxisIndexEntries >= 0, "maxAxisIndexEntries must be non-negative")

object ChunkPlanner:
  def plan(
      grid: RegularGrid,
      selection: ArraySelection,
      limits: PlanningLimits = PlanningLimits()
  ): Either[ZarrError, ReadPlan] = selection match
    case ArraySelection.RegionSelection(region) => planRegion(grid, region, limits)
    case ArraySelection.PointSelection(points) => planPoints(grid, points, limits)
    case ArraySelection.Factored(selection) => planFactored(grid, selection, limits)

  def planFactored(
      grid: RegularGrid,
      selection: FactoredSelection,
      limits: PlanningLimits = PlanningLimits()
  ): Either[ZarrError, ReadPlan] =
    val rank = grid.rank.toInt
    if selection.arrayShape != grid.arrayShape then
      Left(ZarrError.InvalidSelection("factored selection belongs to a different array shape"))
    else if selection.axes.length != rank then
      Left(ZarrError.RankMismatch(rank, selection.axes.length, "factored selection"))
    else
      val axisGroups = new Array[Vector[AxisChunkProjection]](rank)
      var axis = 0
      while axis < rank do
        val groups = planAxis(
          selection.axes(axis),
          grid.arrayShape.axis(axis),
          grid.chunkShape.axis(axis),
          limits
        ) match
          case Left(error) => return Left(error)
          case Right(found) => found
        axisGroups(axis) = groups
        axis += 1

      var chunkTotal = 1L
      axis = 0
      while axis < rank do
        LongArrays.checkedMultiply(
          chunkTotal,
          axisGroups(axis).length.toLong,
          "factored selection chunk count"
        ) match
          case Left(error) => return Left(error)
          case Right(found) => chunkTotal = found
        axis += 1
      if chunkTotal > limits.maxChunks.toLong then
        Left(ZarrError.ResourceLimit("planned chunks", limits.maxChunks, chunkTotal))
      else if chunkTotal == 0L then
        factoredStats(selection, axisGroups, chunkTotal).map: stats =>
          ReadPlan(
            Vector.empty,
            ReadPlanStats(0, selection.requestedElements),
            Some(stats)
          )
      else if rank == 0 then
        val copy = FactoredChunkCopy(Vector.empty, Shape.unsafe(Array.emptyLongArray), 1L)
        factoredStats(selection, axisGroups, chunkTotal).map: stats =>
          ReadPlan(
            Vector(ChunkDemand(
              ChunkCoordinate.unsafe(Array.emptyLongArray),
              ChunkCopy.FactoredCopy(copy)
            )),
            ReadPlanStats(1, selection.requestedElements),
            Some(stats)
          )
      else
        val cursors = new Array[Int](rank)
        val demands = Vector.newBuilder[ChunkDemand]
        var done = false
        while !done do
          val coordinate = new Array[Long](rank)
          val projections = Vector.newBuilder[AxisProjection]
          val fragmentDimensions = new Array[Long](rank)
          var elementCount = 1L
          axis = 0
          while axis < rank do
            val group = axisGroups(axis)(cursors(axis))
            coordinate(axis) = group.chunk
            projections += group.projection
            fragmentDimensions(axis) = group.projection.length
            LongArrays.checkedMultiply(
              elementCount,
              group.projection.length,
              "factored fragment element count"
            ) match
              case Left(error) => return Left(error)
              case Right(found) => elementCount = found
            axis += 1
          demands += ChunkDemand(
            ChunkCoordinate.unsafe(coordinate),
            ChunkCopy.FactoredCopy(FactoredChunkCopy(
              projections.result(),
              Shape.unsafe(fragmentDimensions),
              elementCount
            ))
          )
          var cursorAxis = rank - 1
          var advanced = false
          while cursorAxis >= 0 && !advanced do
            cursors(cursorAxis) += 1
            if cursors(cursorAxis) < axisGroups(cursorAxis).length then advanced = true
            else
              cursors(cursorAxis) = 0
              cursorAxis -= 1
          if !advanced then done = true
        factoredStats(selection, axisGroups, chunkTotal).map: stats =>
          ReadPlan(
            demands.result(),
            ReadPlanStats(chunkTotal.toInt, selection.requestedElements),
            Some(stats)
          )

  def planRegion(
      grid: RegularGrid,
      region: Region,
      limits: PlanningLimits = PlanningLimits()
  ): Either[ZarrError, ReadPlan] =
    val rank = grid.rank.toInt
    if region.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, region.rank.toInt, "region selection"))
    else
      region.extent.elementCount.flatMap: requested =>
        if region.isEmpty then Right(ReadPlan(Vector.empty, ReadPlanStats(0, requested)))
        else
          val first = new Array[Long](rank)
          val last = new Array[Long](rank)
          var axis = 0
          while axis < rank do
            first(axis) = region.origin.values(axis) / grid.chunkShape.values(axis)
            last(axis) =
              (region.origin.values(axis) + region.extent.values(axis) - 1L) /
                grid.chunkShape.values(axis)
            axis += 1

          chunkCount(first, last).flatMap: count =>
            if count > limits.maxChunks.toLong then
              Left(ZarrError.ResourceLimit("planned chunks", limits.maxChunks, count))
            else
              val demands = Vector.newBuilder[ChunkDemand]
              enumerate(first, last): coordinateValues =>
                val chunkOrigin = new Array[Long](rank)
                val sourceOrigin = new Array[Long](rank)
                val destinationOrigin = new Array[Long](rank)
                val extent = new Array[Long](rank)
                var innerAxis = 0
                while innerAxis < rank do
                  val origin = coordinateValues(innerAxis) * grid.chunkShape.values(innerAxis)
                  chunkOrigin(innerAxis) = origin
                  val start = math.max(origin, region.origin.values(innerAxis))
                  val chunkEnd = math.min(origin + grid.chunkShape.values(innerAxis), grid.arrayShape.values(innerAxis))
                  val regionEnd = region.origin.values(innerAxis) + region.extent.values(innerAxis)
                  val end = math.min(chunkEnd, regionEnd)
                  sourceOrigin(innerAxis) = start - origin
                  destinationOrigin(innerAxis) = start - region.origin.values(innerAxis)
                  extent(innerAxis) = end - start
                  innerAxis += 1
                demands += ChunkDemand(
                  ChunkCoordinate.unsafe(coordinateValues),
                  ChunkCopy.RegionCopy(CopyRegion(
                    Coordinate.unsafe(sourceOrigin),
                    Coordinate.unsafe(destinationOrigin),
                    Shape.unsafe(extent)
                  ))
                )
              Right(ReadPlan(demands.result(), ReadPlanStats(count.toInt, requested)))

  def planPoints(
      grid: RegularGrid,
      points: CoordinateBatch,
      limits: PlanningLimits = PlanningLimits()
  ): Either[ZarrError, ReadPlan] =
    val rank = grid.rank.toInt
    if points.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, points.rank.toInt, "point selection"))
    else if points.shape != grid.arrayShape then
      Left(ZarrError.InvalidSelection("point batch belongs to a different array shape"))
    else
      val grouped = mutable.ArrayBuffer.empty[(Array[Long], mutable.ArrayBuffer[(Int, Int)])]
      val lookup = mutable.HashMap.empty[Vector[Long], Int]
      var point = 0
      while point < points.count do
        val chunkCoordinate = new Array[Long](rank)
        val withinChunk = new Array[Long](rank)
        var axis = 0
        while axis < rank do
          val coordinate = points.flattened(point * rank + axis)
          val chunkSize = grid.chunkShape.values(axis)
          chunkCoordinate(axis) = coordinate / chunkSize
          withinChunk(axis) = coordinate % chunkSize
          axis += 1
        linearOffset(grid.chunkShape, withinChunk) match
          case Left(error) => return Left(error)
          case Right(offset) =>
            if offset > Int.MaxValue.toLong then
              return Left(ZarrError.ResourceLimit("chunk element offset", Int.MaxValue, offset))
            val key = chunkCoordinate.toVector
            val groupIndex = lookup.get(key) match
              case Some(index) => index
              case None =>
                val index = grouped.size
                grouped += ((chunkCoordinate, mutable.ArrayBuffer.empty))
                lookup.update(key, index)
                index
            grouped(groupIndex)._2 += ((offset.toInt, point))
        point += 1

      if grouped.size > limits.maxChunks then
        Left(ZarrError.ResourceLimit("planned chunks", limits.maxChunks, grouped.size))
      else
        val sorted = grouped.sortWith: (left, right) =>
          LongArrays.compare(left._1, right._1) < 0
        val demands = sorted.iterator.map: (coordinate, offsets) =>
          val source = new Array[Int](offsets.size)
          val destination = new Array[Int](offsets.size)
          var index = 0
          while index < offsets.size do
            source(index) = offsets(index)._1
            destination(index) = offsets(index)._2
            index += 1
          ChunkDemand(
            ChunkCoordinate.unsafe(coordinate),
            ChunkCopy.GatherCopy(GatherOffsets(
              IntOffsets.unsafe(source),
              IntOffsets.unsafe(destination)
            ))
          )
        Right(ReadPlan(demands.toVector, ReadPlanStats(grouped.size, points.count.toLong)))

  private final case class AxisChunkProjection(chunk: Long, projection: AxisProjection)

  private def factoredStats(
      selection: FactoredSelection,
      groups: Array[Vector[AxisChunkProjection]],
      chunkCount: Long
  ): Either[ZarrError, FactoredPlanStats] =
    var axisGroups = 0L
    var explicitEntries = 0L
    var axis = 0
    while axis < groups.length do
      axisGroups += groups(axis).length.toLong
      selection.axes(axis) match
        case BoundAxisSelection.Gather(indices) => explicitEntries += indices.length.toLong
        case BoundAxisSelection.Strided(_, _, _) => ()
      axis += 1
    if axisGroups > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("factored axis groups", Int.MaxValue, axisGroups))
    else LongArrays.checkedMultiply(
      chunkCount,
      selection.axes.length.toLong,
      "factored demand axis references"
    ).map: references =>
      FactoredPlanStats(axisGroups.toInt, explicitEntries, references)

  private def planAxis(
      selection: BoundAxisSelection,
      dimension: Long,
      chunkSize: Long,
      limits: PlanningLimits
  ): Either[ZarrError, Vector[AxisChunkProjection]] = selection match
    case BoundAxisSelection.Strided(start, step, length) =>
      val result = Vector.newBuilder[AxisChunkProjection]
      var outputStart = 0L
      var groups = 0
      while outputStart < length do
        val scaled = LongArrays.checkedMultiply(outputStart, step, "factored slice coordinate") match
          case Left(error) => return Left(error)
          case Right(found) => found
        val coordinate = LongArrays.checkedAdd(start, scaled, "factored slice coordinate") match
          case Left(error) => return Left(error)
          case Right(found) => found
        val chunk = coordinate / chunkSize
        val chunkOrigin = chunk * chunkSize
        val remainingDimension = dimension - chunkOrigin
        val chunkExtent = math.min(chunkSize, remainingDimension)
        val available = chunkOrigin + chunkExtent - 1L - coordinate
        val inChunk = math.min(length - outputStart, 1L + available / step)
        result += AxisChunkProjection(
          chunk,
          AxisProjection(
            IndexRun.Affine(coordinate - chunkOrigin, step, inChunk),
            IndexRun.Affine(outputStart, 1L, inChunk)
          )
        )
        groups += 1
        if groups > limits.maxChunks then
          return Left(ZarrError.ResourceLimit("axis chunk groups", limits.maxChunks, groups))
        outputStart += inChunk
      Right(result.result())
    case BoundAxisSelection.Gather(indices) =>
      if indices.length > limits.maxAxisIndexEntries then
        Left(ZarrError.ResourceLimit(
          "axis index entries",
          limits.maxAxisIndexEntries,
          indices.length
        ))
      else
        val grouped = mutable.HashMap.empty[Long, (mutable.ArrayBuffer[Long], mutable.ArrayBuffer[Long])]
        var output = 0
        while output < indices.length do
          val coordinate = indices(output)
          val chunk = coordinate / chunkSize
          val values = grouped.getOrElseUpdate(
            chunk,
            mutable.ArrayBuffer.empty[Long] -> mutable.ArrayBuffer.empty[Long]
          )
          values._1 += coordinate % chunkSize
          values._2 += output.toLong
          output += 1
        if grouped.size > limits.maxChunks then
          Left(ZarrError.ResourceLimit("axis chunk groups", limits.maxChunks, grouped.size))
        else Right(grouped.keys.toVector.sorted.map: chunk =>
          val (source, destination) = grouped(chunk)
          AxisChunkProjection(
            chunk,
            AxisProjection(
              IndexRun.Explicit(LongOffsets.unsafe(source.toArray)),
              IndexRun.Explicit(LongOffsets.unsafe(destination.toArray))
            )
          )
        )

  private def chunkCount(first: Array[Long], last: Array[Long]): Either[ZarrError, Long] =
    var count = 1L
    var axis = 0
    while axis < first.length do
      val length = last(axis) - first(axis) + 1L
      LongArrays.checkedMultiply(count, length, "planned chunk count") match
        case Left(error) => return Left(error)
        case Right(value) => count = value
      axis += 1
    Right(count)

  private def enumerate(first: Array[Long], last: Array[Long])(
      consume: Array[Long] => Unit
  ): Unit =
    if first.length == 0 then consume(Array.emptyLongArray)
    else
      val current = LongArrays.copy(first)
      var done = false
      while !done do
        consume(LongArrays.copy(current))
        var axis = current.length - 1
        var advanced = false
        while axis >= 0 && !advanced do
          if current(axis) < last(axis) then
            current(axis) += 1L
            var reset = axis + 1
            while reset < current.length do
              current(reset) = first(reset)
              reset += 1
            advanced = true
          else axis -= 1
        if !advanced then done = true

  private def linearOffset(shape: Shape, coordinate: Array[Long]): Either[ZarrError, Long] =
    var offset = 0L
    var axis = 0
    while axis < shape.rank.toInt do
      LongArrays.checkedMultiply(offset, shape.values(axis), "C-order chunk offset") match
        case Left(error) => return Left(error)
        case Right(scaled) =>
          LongArrays.checkedAdd(scaled, coordinate(axis), "C-order chunk offset") match
            case Left(error) => return Left(error)
            case Right(value) => offset = value
      axis += 1
    Right(offset)
