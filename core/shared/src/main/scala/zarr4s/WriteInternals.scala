package zarr4s

private[zarr4s] object WriteInternals:
  def arrayMetadata(
      descriptor: ArrayDescriptor,
      path: ZarrPath,
      limits: WriterLimits
  ): Either[ZarrError, (StoreKey, OwnedBytes)] =
    ZarrMetadataRenderer
      .array(descriptor)
      .flatMap: rendered =>
        metadata(rendered, path, limits)

  def groupMetadata(
      group: GroupMetadata,
      path: ZarrPath,
      limits: WriterLimits
  ): Either[ZarrError, (StoreKey, OwnedBytes)] =
    ZarrMetadataRenderer
      .group(group)
      .flatMap: rendered =>
        metadata(rendered, path, limits)

  private def metadata(
      rendered: String,
      path: ZarrPath,
      limits: WriterLimits
  ): Either[ZarrError, (StoreKey, OwnedBytes)] =
    val bytes = OwnedBytes.unsafe(rendered.getBytes("UTF-8"))
    if bytes.byteCount.toLong > limits.maxMetadataBytes.toLong then
      Left(
        ZarrError.ResourceLimit(
          "metadata bytes",
          limits.maxMetadataBytes.toLong,
          bytes.byteCount.toLong
        )
      )
    else if limits.maxObjects < 1 then
      Left(ZarrError.ResourceLimit("written objects", limits.maxObjects, 1L))
    else if bytes.byteCount.toLong > limits.maxWrittenBytes.toLong then
      Left(
        ZarrError.ResourceLimit(
          "written bytes",
          limits.maxWrittenBytes.toLong,
          bytes.byteCount.toLong
        )
      )
    else path.key("zarr.json").map(_ -> bytes)

  def resolve(path: ZarrPath, relative: StoreKey): Either[ZarrError, StoreKey] =
    path.key(relative.value)

  def written(key: StoreKey, bytes: OwnedBytes): WrittenObject =
    WrittenObject(key, bytes.byteCount, PortableSha256.digest(bytes))

  def foreachCoordinate(
      shape: Shape
  )(operation: ChunkCoordinate => Either[ZarrError, Unit]): Either[ZarrError, Unit] =
    if shape.values.exists(_ == 0L) then Right(())
    else if shape.rank.toInt == 0 then operation(ChunkCoordinate.unsafe(Array.emptyLongArray))
    else
      val current = new Array[Long](shape.rank.toInt)
      var done = false
      while !done do
        operation(ChunkCoordinate.unsafe(current)) match
          case Left(error) => return Left(error)
          case Right(_)    => ()
        advance(current, shape)
        done = current.forall(_ == 0L)
      Right(())

  /** Advances a non-empty coordinate and wraps to zero after the last value. */
  def advance(current: Array[Long], shape: Shape): Unit =
    var axis = current.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      current(axis) += 1L
      if current(axis) < shape.values(axis) then advanced = true
      else
        current(axis) = 0L
        axis -= 1

  def globalInnerCoordinate(
      shard: ChunkCoordinate,
      local: ChunkCoordinate,
      grid: ShardedGrid
  ): ChunkCoordinate =
    val values = new Array[Long](grid.rank.toInt)
    var axis = 0
    while axis < values.length do
      values(axis) = shard.values(axis) * grid.innerChunksPerShard.values(axis) + local.values(axis)
      axis += 1
    ChunkCoordinate.unsafe(values)

  def insideGrid(coordinate: ChunkCoordinate, shape: Shape): Boolean =
    var axis = 0
    while axis < shape.rank.toInt do
      if coordinate.values(axis) >= shape.values(axis) then return false
      axis += 1
    true

  def assembleShard(
      chunks: Vector[Option[OwnedBytes]],
      innerChunksPerShard: Shape,
      location: IndexLocation,
      limits: WriterLimits
  ): Either[ZarrError, Option[OwnedBytes]] =
    if chunks.forall(_.isEmpty) then Right(None)
    else
      ShardIndexCodec
        .encodedLength(innerChunksPerShard, limits.shardIndex)
        .flatMap: indexLength =>
          shardDataLength(chunks).flatMap: dataLength =>
            LongArrays
              .checkedAdd(dataLength, indexLength.toLong, "encoded shard bytes")
              .flatMap: total =>
                val limit = math.min(limits.maxShardBytes.toLong, Int.MaxValue.toLong)
                if total > limit then
                  Left(ZarrError.ResourceLimit("encoded shard bytes", limit, total))
                else
                  val entries = Vector.newBuilder[ShardIndexEntry]
                  var offset = if location == IndexLocation.Start then indexLength.toLong else 0L
                  chunks.foreach:
                    case None        => entries += ShardIndexEntry.Fill
                    case Some(bytes) =>
                      entries += ShardIndexEntry.Present(offset, bytes.byteCount)
                      offset += bytes.byteCount.toLong
                  ShardIndex(innerChunksPerShard, entries.result(), limits.shardIndex).flatMap:
                    shardIndex =>
                      ShardIndexCodec
                        .encode(shardIndex, limits.shardIndex)
                        .map: encodedIndex =>
                          val output = new Array[Byte](total.toInt)
                          var position = 0
                          if location == IndexLocation.Start then
                            Array.copy(encodedIndex.values, 0, output, 0, encodedIndex.length)
                            position = encodedIndex.length
                          chunks.foreach:
                            case None        => ()
                            case Some(bytes) =>
                              Array.copy(bytes.values, 0, output, position, bytes.length)
                              position += bytes.length
                          if location == IndexLocation.End then
                            Array.copy(
                              encodedIndex.values,
                              0,
                              output,
                              position,
                              encodedIndex.length
                            )
                          Some(OwnedBytes.unsafe(output))

  private def shardDataLength(
      chunks: Vector[Option[OwnedBytes]]
  ): Either[ZarrError, Long] =
    var length = 0L
    var index = 0
    var failure: Option[ZarrError] = None
    while index < chunks.length && failure.isEmpty do
      chunks(index) match
        case None        => ()
        case Some(bytes) =>
          LongArrays.checkedAdd(
            length,
            bytes.byteCount.toLong,
            "encoded shard data bytes"
          ) match
            case Left(error)  => failure = Some(error)
            case Right(found) => length = found
      index += 1
    failure.toLeft(length)
