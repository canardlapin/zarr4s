package scalafim.zarr

private[zarr] object ChunkGeometry:
  /** The decoded shape of every regular-grid chunk, including border chunks.
    *
    * Zarr v3 border chunks retain the grid's nominal chunk shape. Elements
    * outside the logical array extent are represented by the array fill value.
    */
  def storedShape(grid: RegularGrid, coordinate: ChunkCoordinate): Either[ZarrError, Shape] =
    val rank = grid.rank.toInt
    if coordinate.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, coordinate.rank.toInt, "chunk shape"))
    else
      var axis = 0
      while axis < rank do
        val index = coordinate.values(axis)
        if index < 0L || index >= grid.gridShape.values(axis) then
          return Left(ZarrError.OutOfBounds(
            s"chunk index $index on axis $axis outside grid length ${grid.gridShape.values(axis)}"
          ))
        axis += 1
      Right(grid.chunkShape)

private[zarr] sealed trait PrimitiveBlockBuilder:
  def size: Int
  def copyElement(source: PrimitiveBlock, sourceIndex: Int, destinationIndex: Int): Either[ZarrError, Unit]
  def result(): PrimitiveBlock

private[zarr] object PrimitiveBlockBuilder:
  def apply(
      dataType: DataTypeCapability,
      fill: StoredScalar,
      shape: Shape
  ): Either[ZarrError, PrimitiveBlockBuilder] = shape.elementCount.flatMap: count =>
    if count > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("materialized result elements", Int.MaxValue, count))
    else dataType.scalarKind.allocate(fill, count.toInt, dataType.name).map(GenericBuilder.apply)

  def applyCopy(
      builder: PrimitiveBlockBuilder,
      source: PrimitiveBlock,
      sourceShape: Shape,
      destinationShape: Shape,
      copy: ChunkCopy
  ): Either[ZarrError, Unit] = copy match
    case ChunkCopy.GatherCopy(offsets) =>
      var index = 0
      while index < offsets.source.size do
        builder.copyElement(source, offsets.source(index), offsets.destination(index)) match
          case Left(error) => return Left(error)
          case Right(_) => ()
        index += 1
      Right(())
    case ChunkCopy.RegionCopy(region) =>
      copyRegion(builder, source, sourceShape, destinationShape, region)
    case ChunkCopy.FactoredCopy(factored) =>
      copyFactored(builder, source, sourceShape, destinationShape, factored, compact = false)

  def compact(
      dataType: DataTypeCapability,
      fill: StoredScalar,
      source: PrimitiveBlock,
      sourceShape: Shape,
      copy: FactoredChunkCopy
  ): Either[ZarrError, PrimitiveBlock] =
    apply(dataType, fill, copy.fragmentShape).flatMap: builder =>
      copyFactored(
        builder,
        source,
        sourceShape,
        copy.fragmentShape,
        copy,
        compact = true
      ).map(_ => builder.result())

  def applyFragment(
      builder: PrimitiveBlockBuilder,
      fragment: ChunkFragment,
      destinationShape: Shape
  ): Either[ZarrError, Unit] =
    val rank = fragment.shape.rank.toInt
    if destinationShape.rank.toInt != rank || fragment.placement.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, destinationShape.rank.toInt, "fragment placement"))
    else
      val cursor = new Array[Long](rank)
      var sourceOffset = 0
      while sourceOffset < fragment.elementCount do
        var destinationOffset = 0L
        var axis = 0
        while axis < rank do
          destinationOffset = destinationOffset * destinationShape.axis(axis) +
            fragment.placement.unsafeOutputIndex(axis, cursor(axis))
          axis += 1
        if destinationOffset > Int.MaxValue.toLong then
          return Left(ZarrError.ResourceLimit(
            "fragment destination offset",
            Int.MaxValue,
            destinationOffset
          ))
        builder.copyElement(fragment.values, sourceOffset, destinationOffset.toInt) match
          case Left(error) => return Left(error)
          case Right(_) => ()
        sourceOffset += 1
        advance(cursor, fragment.shape)
      Right(())

  private def copyFactored(
      builder: PrimitiveBlockBuilder,
      source: PrimitiveBlock,
      sourceShape: Shape,
      destinationShape: Shape,
      copy: FactoredChunkCopy,
      compact: Boolean
  ): Either[ZarrError, Unit] =
    val rank = sourceShape.rank.toInt
    if copy.axes.length != rank || destinationShape.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, copy.axes.length, "factored chunk copy"))
    else
      val cursor = new Array[Long](rank)
      var copied = 0L
      while copied < copy.elementCount do
        var sourceOffset = 0L
        var destinationOffset = 0L
        var axis = 0
        while axis < rank do
          val projection = copy.axes(axis)
          val destination =
            if compact then cursor(axis)
            else projection.destination(cursor(axis))
          sourceOffset = sourceOffset * sourceShape.axis(axis) + projection.source(cursor(axis))
          destinationOffset = destinationOffset * destinationShape.axis(axis) + destination
          axis += 1
        if sourceOffset > Int.MaxValue.toLong || destinationOffset > Int.MaxValue.toLong then
          return Left(ZarrError.ResourceLimit(
            "factored copy element offset",
            Int.MaxValue,
            math.max(sourceOffset, destinationOffset)
          ))
        builder.copyElement(source, sourceOffset.toInt, destinationOffset.toInt) match
          case Left(error) => return Left(error)
          case Right(_) => ()
        copied += 1L
        advance(cursor, copy.fragmentShape)
      Right(())

  private def copyRegion(
      builder: PrimitiveBlockBuilder,
      source: PrimitiveBlock,
      sourceShape: Shape,
      destinationShape: Shape,
      copy: CopyRegion
  ): Either[ZarrError, Unit] =
    val rank = sourceShape.rank.toInt
    if destinationShape.rank.toInt != rank || copy.extent.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, destinationShape.rank.toInt, "chunk copy"))
    else copy.extent.elementCount match
      case Left(error) => Left(error)
      case Right(elementCount) =>
        val cursor = new Array[Long](rank)
        var copied = 0L
        while copied < elementCount do
          var sourceOffset = 0L
          var destinationOffset = 0L
          var axis = 0
          while axis < rank do
            sourceOffset = sourceOffset * sourceShape.values(axis) +
              copy.sourceOrigin.values(axis) + cursor(axis)
            destinationOffset = destinationOffset * destinationShape.values(axis) +
              copy.destinationOrigin.values(axis) + cursor(axis)
            axis += 1
          if sourceOffset > Int.MaxValue.toLong || destinationOffset > Int.MaxValue.toLong then
            return Left(ZarrError.ResourceLimit(
              "copy element offset",
              Int.MaxValue,
              math.max(sourceOffset, destinationOffset)
            ))
          builder.copyElement(source, sourceOffset.toInt, destinationOffset.toInt) match
            case Left(error) => return Left(error)
            case Right(_) => ()
          copied += 1L
          advance(cursor, copy.extent)
        Right(())

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.values(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  private final class GenericBuilder private (block: PrimitiveBlock) extends PrimitiveBlockBuilder:
    val size = block.elementCount

    def copyElement(
        source: PrimitiveBlock,
        sourceIndex: Int,
        destinationIndex: Int
    ): Either[ZarrError, Unit] = block.copyElementFrom(source, sourceIndex, destinationIndex)

    def result(): PrimitiveBlock = block

  private object GenericBuilder:
    def apply(block: PrimitiveBlock): GenericBuilder = new GenericBuilder(block)
