package zarr4s

/** A lazy provider backed by one validated dense value.
  *
  * The source is converted to the existing PrimitiveBlock representation once when the provider is
  * constructed. Each request then allocates only its nominal stored chunk, fills it from the
  * descriptor's fill value, and copies the logical intersection for that coordinate. No complete
  * chunk-coordinate list or encoded object list is materialized.
  */
private[zarr4s] final class DenseChunkProvider private[zarr4s] (
    descriptor: ArrayDescriptor,
    source: PrimitiveBlock,
    sourceShape: Shape,
    chunkShape: Shape
) extends ChunkProvider:
  def chunk(
      coordinate: ChunkCoordinate,
      storedShape: Shape
  ): Either[ZarrError, ChunkPayload] =
    val grid = ChunkProvider.logicalGrid(descriptor)
    if coordinate.rank.toInt != grid.rank.toInt then
      Left(ZarrError.RankMismatch(grid.rank.toInt, coordinate.rank.toInt, "dense chunk coordinate"))
    else if storedShape != chunkShape then
      Left(
        ZarrError.InvalidGrid(
          s"dense provider expected stored shape $chunkShape, found $storedShape"
        )
      )
    else
      validateCoordinate(grid, coordinate).flatMap: _ =>
        for
          origin <- chunkOrigin(chunkShape, coordinate)
          extent <- logicalExtent(sourceShape, origin, chunkShape)
          builder <- PrimitiveBlockBuilder(
            descriptor.dataType,
            descriptor.fillValue,
            storedShape
          )
          _ <- PrimitiveBlockBuilder.applyCopy(
            builder,
            source,
            sourceShape,
            storedShape,
            ChunkCopy.RegionCopy(
              CopyRegion(
                origin,
                Coordinate.unsafe(Array.fill(storedShape.rank.toInt)(0L)),
                extent
              )
            )
          )
        yield ChunkPayload.Values(builder.result())

  private def validateCoordinate(
      grid: RegularGrid,
      coordinate: ChunkCoordinate
  ): Either[ZarrError, Unit] =
    var axis = 0
    while axis < grid.rank.toInt do
      val value = coordinate.axis(axis)
      if value < 0L || value >= grid.gridShape.axis(axis) then
        return Left(
          ZarrError.OutOfBounds(
            s"dense chunk coordinate axis $axis value $value outside grid length ${grid.gridShape.axis(axis)}"
          )
        )
      axis += 1
    Right(())

  private def chunkOrigin(
      shape: Shape,
      coordinate: ChunkCoordinate
  ): Either[ZarrError, Coordinate] =
    val values = new Array[Long](shape.rank.toInt)
    var axis = 0
    while axis < values.length do
      LongArrays.checkedMultiply(
        coordinate.axis(axis),
        shape.axis(axis),
        s"dense chunk origin axis $axis"
      ) match
        case Left(error)  => return Left(error)
        case Right(found) => values(axis) = found
      axis += 1
    Right(Coordinate.unsafe(values))

  private def logicalExtent(
      arrayShape: Shape,
      origin: Coordinate,
      chunkShape: Shape
  ): Either[ZarrError, Shape] =
    val values = new Array[Long](arrayShape.rank.toInt)
    var axis = 0
    while axis < values.length do
      val remaining = arrayShape.axis(axis) - origin.axis(axis)
      values(axis) = math.max(0L, math.min(chunkShape.axis(axis), remaining))
      axis += 1
    Right(Shape.unsafe(values))
