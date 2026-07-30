package zarr4s

import scala.collection.mutable

enum IndexLocation:
  case Start
  case End

enum ShardIndexEntry:
  case Fill
  case Present(offset: Long, length: ByteCount)

final case class ShardIndexLimits(
    maxEntries: Int = 1000000,
    maxIndexBytes: ByteCount = ByteCount.unsafe(64L * 1024L * 1024L),
    maxShardBytes: ByteCount = ByteCount.unsafe(4L * 1024L * 1024L * 1024L)
):
  require(maxEntries >= 0, "maxEntries must be non-negative")

final class ShardIndex private (
    val innerGridShape: Shape,
    val entries: Vector[ShardIndexEntry]
):
  def entry(coordinate: ChunkCoordinate): Either[ZarrError, ShardIndexEntry] =
    ShardIndex.linearIndex(innerGridShape, coordinate).map(entries.apply)

object ShardIndex:
  def apply(
      innerGridShape: Shape,
      entries: Vector[ShardIndexEntry],
      limits: ShardIndexLimits = ShardIndexLimits()
  ): Either[ZarrError, ShardIndex] = innerGridShape.elementCount match
    case Left(error)     => Left(error)
    case Right(expected) =>
      if expected > limits.maxEntries.toLong then
        Left(ZarrError.ResourceLimit("shard index entries", limits.maxEntries, expected))
      else if entries.length.toLong != expected then
        Left(
          ZarrError.InvalidSelection(
            s"shard index requires $expected entries, found ${entries.length}"
          )
        )
      else
        var index = 0
        while index < entries.length do
          entries(index) match
            case ShardIndexEntry.Fill                    => ()
            case ShardIndexEntry.Present(offset, length) =>
              if offset < 0L then
                return Left(ZarrError.InvalidSelection(s"negative shard offset at entry $index"))
              LongArrays.checkedAdd(offset, length.toLong, s"shard entry $index end") match
                case Left(error)                                     => return Left(error)
                case Right(end) if end > limits.maxShardBytes.toLong =>
                  return Left(
                    ZarrError.ResourceLimit(
                      "shard entry end",
                      limits.maxShardBytes.toLong,
                      end
                    )
                  )
                case Right(_) => ()
          index += 1
        Right(new ShardIndex(innerGridShape, entries))

  private[zarr4s] def linearIndex(
      shape: Shape,
      coordinate: ChunkCoordinate
  ): Either[ZarrError, Int] =
    val rank = shape.rank.toInt
    if coordinate.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, coordinate.rank.toInt, "shard index coordinate"))
    else
      var offset = 0L
      var axis = 0
      while axis < rank do
        val value = coordinate.values(axis)
        if value < 0L || value >= shape.values(axis) then
          return Left(
            ZarrError.OutOfBounds(
              s"inner chunk index $value on axis $axis outside length ${shape.values(axis)}"
            )
          )
        LongArrays.checkedMultiply(offset, shape.values(axis), "shard index offset") match
          case Left(error)   => return Left(error)
          case Right(scaled) =>
            LongArrays.checkedAdd(scaled, value, "shard index offset") match
              case Left(error)  => return Left(error)
              case Right(found) => offset = found
        axis += 1
      if offset > Int.MaxValue.toLong then
        Left(ZarrError.ResourceLimit("shard index offset", Int.MaxValue, offset))
      else Right(offset.toInt)

object ShardIndexCodec:
  private val entryBytes = 16L
  private val checksumBytes = 4L

  def encodedLength(
      innerGridShape: Shape,
      limits: ShardIndexLimits = ShardIndexLimits()
  ): Either[ZarrError, ByteCount] =
    for
      entries <- innerGridShape.elementCount
      _ <-
        if entries <= limits.maxEntries.toLong then Right(())
        else Left(ZarrError.ResourceLimit("shard index entries", limits.maxEntries, entries))
      payload <- LongArrays.checkedMultiply(entries, entryBytes, "shard index byte length")
      total <- LongArrays.checkedAdd(payload, checksumBytes, "shard index byte length")
      _ <-
        if total <= limits.maxIndexBytes.toLong then Right(())
        else Left(ZarrError.ResourceLimit("shard index bytes", limits.maxIndexBytes.toLong, total))
      count <- ByteCount(total)
    yield count

  def encode(
      index: ShardIndex,
      limits: ShardIndexLimits = ShardIndexLimits()
  ): Either[ZarrError, OwnedBytes] = encodedLength(index.innerGridShape, limits).flatMap: length =>
    if length.toLong > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("materialized index bytes", Int.MaxValue, length.toLong))
    else
      val bytes = new Array[Byte](length.toLong.toInt)
      var entryIndex = 0
      while entryIndex < index.entries.length do
        val offset = entryIndex * 16
        index.entries(entryIndex) match
          case ShardIndexEntry.Fill =>
            putUInt64Max(bytes, offset)
            putUInt64Max(bytes, offset + 8)
          case ShardIndexEntry.Present(chunkOffset, chunkLength) =>
            putLittleEndianUInt64(bytes, offset, chunkOffset)
            putLittleEndianUInt64(bytes, offset + 8, chunkLength.toLong)
        entryIndex += 1
      val payloadLength = bytes.length - 4
      Crc32c.putLittleEndianUInt32(bytes, payloadLength, Crc32c.checksum(bytes, 0, payloadLength))
      Right(OwnedBytes.unsafe(bytes))

  def decode(
      encoded: OwnedBytes,
      innerGridShape: Shape,
      limits: ShardIndexLimits = ShardIndexLimits()
  ): Either[ZarrError, ShardIndex] = encodedLength(innerGridShape, limits) match
    case Left(error)     => Left(error)
    case Right(expected) =>
      if encoded.byteCount != expected then
        Left(
          ZarrError.InvalidSelection(
            s"encoded shard index length must be ${expected.toLong}, found ${encoded.byteCount.toLong}"
          )
        )
      else
        Crc32c.verifyAndStrip(encoded) match
          case Left(error)    => Left(ZarrError.InvalidSelection(error.message))
          case Right(payload) =>
            val entries = Vector.newBuilder[ShardIndexEntry]
            var offset = 0
            while offset < payload.length do
              val chunkOffset = getLittleEndianUInt64(payload.values, offset)
              val chunkLength = getLittleEndianUInt64(payload.values, offset + 8)
              (chunkOffset, chunkLength) match
                case (UInt64.Max, UInt64.Max)          => entries += ShardIndexEntry.Fill
                case (UInt64.Max, _) | (_, UInt64.Max) =>
                  return Left(
                    ZarrError.InvalidSelection(
                      s"partial fill sentinel in shard index entry ${offset / 16}"
                    )
                  )
                case (UInt64.TooLarge, _) | (_, UInt64.TooLarge) =>
                  return Left(
                    ZarrError.ResourceLimit(
                      "unsigned shard index value",
                      Long.MaxValue,
                      Long.MaxValue
                    )
                  )
                case (UInt64.Value(foundOffset), UInt64.Value(foundLength)) =>
                  entries += ShardIndexEntry.Present(foundOffset, ByteCount.unsafe(foundLength))
              offset += 16
            ShardIndex(innerGridShape, entries.result(), limits)

  private enum UInt64:
    case Max
    case TooLarge
    case Value(value: Long)

  private def putUInt64Max(bytes: Array[Byte], offset: Int): Unit =
    var index = 0
    while index < 8 do
      bytes(offset + index) = 0xff.toByte
      index += 1

  private def putLittleEndianUInt64(bytes: Array[Byte], offset: Int, value: Long): Unit =
    require(value >= 0L, "uint64 value must fit a signed long")
    var index = 0
    while index < 8 do
      bytes(offset + index) = ((value >>> (index * 8)) & 0xffL).toByte
      index += 1

  private def getLittleEndianUInt64(bytes: Array[Byte], offset: Int): UInt64 =
    var allMaximum = true
    var index = 0
    while index < 8 do
      if (bytes(offset + index) & 0xff) != 0xff then allMaximum = false
      index += 1
    if allMaximum then UInt64.Max
    else if (bytes(offset + 7) & 0x80) != 0 then UInt64.TooLarge
    else
      var value = 0L
      index = 0
      while index < 8 do
        value |= (bytes(offset + index).toLong & 0xffL) << (index * 8)
        index += 1
      UInt64.Value(value)

final class ShardedGrid private (
    val outerGrid: RegularGrid,
    val innerChunkShape: Shape,
    val innerChunksPerShard: Shape,
    val globalInnerGrid: RegularGrid
):
  val rank: Rank = outerGrid.rank

  def locate(
      globalInnerCoordinate: ChunkCoordinate
  ): Either[ZarrError, (ChunkCoordinate, ChunkCoordinate)] =
    if globalInnerCoordinate.rank.toInt != rank.toInt then
      Left(
        ZarrError
          .RankMismatch(rank.toInt, globalInnerCoordinate.rank.toInt, "inner chunk coordinate")
      )
    else
      val shard = new Array[Long](rank.toInt)
      val local = new Array[Long](rank.toInt)
      var axis = 0
      while axis < rank.toInt do
        val coordinate = globalInnerCoordinate.values(axis)
        if coordinate < 0L || coordinate >= globalInnerGrid.gridShape.values(axis) then
          return Left(
            ZarrError.OutOfBounds(
              s"inner chunk index $coordinate on axis $axis outside grid length ${globalInnerGrid.gridShape.values(axis)}"
            )
          )
        val perShard = innerChunksPerShard.values(axis)
        shard(axis) = coordinate / perShard
        local(axis) = coordinate % perShard
        axis += 1
      Right(ChunkCoordinate.unsafe(shard) -> ChunkCoordinate.unsafe(local))

object ShardedGrid:
  def apply(
      outerGrid: RegularGrid,
      innerChunkShape: Shape
  ): Either[ZarrError, ShardedGrid] =
    val rank = outerGrid.rank.toInt
    if innerChunkShape.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, innerChunkShape.rank.toInt, "inner chunk shape"))
    else
      val chunksPerShard = new Array[Long](rank)
      var axis = 0
      while axis < rank do
        val inner = innerChunkShape.values(axis)
        val outer = outerGrid.chunkShape.values(axis)
        if inner <= 0L then
          return Left(
            ZarrError.InvalidGrid(
              s"inner chunk dimension $axis must be positive, found $inner"
            )
          )
        if outer % inner != 0L then
          return Left(
            ZarrError.InvalidGrid(
              s"inner chunk dimension $inner does not divide shard dimension $outer on axis $axis"
            )
          )
        chunksPerShard(axis) = outer / inner
        axis += 1
      RegularGrid(outerGrid.arrayShape, innerChunkShape).map: globalInnerGrid =>
        new ShardedGrid(
          outerGrid,
          innerChunkShape,
          Shape.unsafe(chunksPerShard),
          globalInnerGrid
        )

final case class InnerChunkDemand(localCoordinate: ChunkCoordinate, copy: ChunkCopy)

final case class ShardDemand(
    coordinate: ChunkCoordinate,
    innerChunks: Vector[InnerChunkDemand]
)

final case class ShardReadPlan(
    shards: Vector[ShardDemand],
    touchedInnerChunks: Int,
    touchedShards: Int
)

object ShardPlanner:
  def group(
      grid: ShardedGrid,
      innerPlan: ReadPlan
  ): Either[ZarrError, ShardReadPlan] =
    val groups = mutable.HashMap.empty[Vector[Long], mutable.ArrayBuffer[InnerChunkDemand]]
    val coordinates = mutable.HashMap.empty[Vector[Long], ChunkCoordinate]
    var demandIndex = 0
    while demandIndex < innerPlan.demands.length do
      val demand = innerPlan.demands(demandIndex)
      grid.locate(demand.coordinate) match
        case Left(error)           => return Left(error)
        case Right((shard, local)) =>
          val key = shard.toVector
          val chunks = groups.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
          chunks += InnerChunkDemand(local, demand.copy)
          coordinates.update(key, shard)
      demandIndex += 1
    val orderedKeys =
      groups.keys.toVector.sortWith((left, right) => compareVectors(left, right) < 0)
    val shards = orderedKeys.map: key =>
      ShardDemand(coordinates(key), groups(key).toVector)
    Right(ShardReadPlan(shards, innerPlan.demands.length, shards.length))

  private def compareVectors(left: Vector[Long], right: Vector[Long]): Int =
    var axis = 0
    while axis < left.length do
      val comparison = java.lang.Long.compare(left(axis), right(axis))
      if comparison != 0 then return comparison
      axis += 1
    0

final case class IndexRangeRead(
    shardCoordinate: ChunkCoordinate,
    key: StoreKey,
    range: ByteRange
)

final case class ShardIndexReadPlan(reads: Vector[IndexRangeRead], indexBytes: ByteCount)

object ShardIndexReadPlan:
  def apply(
      plan: ShardReadPlan,
      innerGridShape: Shape,
      location: IndexLocation,
      keyEncoding: ChunkKeyEncoding,
      objectLengths: Map[Vector[Long], Long] = Map.empty,
      limits: ShardIndexLimits = ShardIndexLimits()
  ): Either[ZarrError, ShardIndexReadPlan] =
    ShardIndexCodec.encodedLength(innerGridShape, limits) match
      case Left(error)        => Left(error)
      case Right(indexLength) =>
        val reads = Vector.newBuilder[IndexRangeRead]
        var index = 0
        while index < plan.shards.length do
          val shard = plan.shards(index)
          val offset = location match
            case IndexLocation.Start => 0L
            case IndexLocation.End   =>
              objectLengths.get(shard.coordinate.toVector) match
                case None =>
                  return Left(
                    ZarrError.InvalidSelection(
                      s"object length is required for end-indexed shard ${shard.coordinate}"
                    )
                  )
                case Some(length) if length < indexLength.toLong =>
                  return Left(
                    ZarrError.InvalidSelection(
                      s"shard length $length is shorter than its ${indexLength.toLong}-byte index"
                    )
                  )
                case Some(length) => length - indexLength.toLong
          val range = ByteRange(offset, indexLength.toLong) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          reads += IndexRangeRead(
            shard.coordinate,
            keyEncoding.encode(shard.coordinate),
            range
          )
          index += 1
        LongArrays
          .checkedMultiply(
            indexLength.toLong,
            plan.shards.length.toLong,
            "total shard index bytes"
          )
          .flatMap(ByteCount.apply)
          .map(total => new ShardIndexReadPlan(reads.result(), total))

enum ShardChunkSource:
  case Fill
  case Range(range: ByteRange)

final case class ShardChunkRead(
    localCoordinate: ChunkCoordinate,
    source: ShardChunkSource,
    copy: ChunkCopy
)

final case class ShardDataRead(
    coordinate: ChunkCoordinate,
    key: StoreKey,
    chunks: Vector[ShardChunkRead]
)

final case class ShardDataPlan(
    shards: Vector[ShardDataRead],
    rangeReads: Int,
    plannedBytes: ByteCount
)

object ShardDataPlan:
  def resolve(
      plan: ShardReadPlan,
      indexes: Map[Vector[Long], ShardIndex],
      keyEncoding: ChunkKeyEncoding
  ): Either[ZarrError, ShardDataPlan] =
    val shards = Vector.newBuilder[ShardDataRead]
    var rangeReads = 0
    var plannedBytes = 0L
    var shardIndex = 0
    while shardIndex < plan.shards.length do
      val shard = plan.shards(shardIndex)
      val index = indexes.get(shard.coordinate.toVector) match
        case None =>
          return Left(
            ZarrError.InvalidSelection(s"missing decoded index for shard ${shard.coordinate}")
          )
        case Some(found) => found
      val chunks = Vector.newBuilder[ShardChunkRead]
      var chunkIndex = 0
      while chunkIndex < shard.innerChunks.length do
        val inner = shard.innerChunks(chunkIndex)
        index.entry(inner.localCoordinate) match
          case Left(error)                 => return Left(error)
          case Right(ShardIndexEntry.Fill) =>
            chunks += ShardChunkRead(inner.localCoordinate, ShardChunkSource.Fill, inner.copy)
          case Right(ShardIndexEntry.Present(offset, length)) =>
            val range = ByteRange(offset, length.toLong) match
              case Left(error)  => return Left(error)
              case Right(found) => found
            LongArrays.checkedAdd(plannedBytes, length.toLong, "planned shard bytes") match
              case Left(error)  => return Left(error)
              case Right(found) => plannedBytes = found
            rangeReads += 1
            chunks += ShardChunkRead(
              inner.localCoordinate,
              ShardChunkSource.Range(range),
              inner.copy
            )
        chunkIndex += 1
      shards += ShardDataRead(
        shard.coordinate,
        keyEncoding.encode(shard.coordinate),
        chunks.result()
      )
      shardIndex += 1
    ByteCount(plannedBytes).map: bytes =>
      ShardDataPlan(shards.result(), rangeReads, bytes)
