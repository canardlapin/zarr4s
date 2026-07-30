package zarr4s

final case class OpenLimits(
    maxMetadataBytes: ByteCount = ByteCount.unsafe(4L * 1024L * 1024L),
    maxRank: Int = 32,
    maxDecodedChunkBytes: ByteCount = ByteCount.unsafe(512L * 1024L * 1024L),
    hierarchy: HierarchyLimits = HierarchyLimits()
):
  require(maxRank >= 0, "maxRank must be non-negative")

final case class ReadLimits(
    maxObjects: Int = 100000,
    maxRanges: Int = 1000000,
    maxConcurrentRequests: Int = 8,
    maxEncodedObjectBytes: ByteCount = ByteCount.unsafe(512L * 1024L * 1024L),
    decode: DecodeLimits = DecodeLimits.default,
    shardIndex: ShardIndexLimits = ShardIndexLimits(),
    coalescing: CoalescingLimits = CoalescingLimits.default,
    planning: PlanningLimits = PlanningLimits()
):
  require(maxObjects >= 0, "maxObjects must be non-negative")
  require(maxRanges >= 0, "maxRanges must be non-negative")
  require(maxConcurrentRequests > 0, "maxConcurrentRequests must be positive")

final case class ExecutionReceipt private[zarr4s] (
    objectRequests: Int,
    rangeRequests: Int,
    lengthRequests: Int,
    bytesRead: Long,
    indexBytesRead: Long,
    dataBytesRead: Long,
    touchedChunks: Int,
    touchedShards: Int,
    requestedElements: Long,
    elementByteWidth: Int
):
  require(objectRequests >= 0, "objectRequests must be non-negative")
  require(rangeRequests >= 0, "rangeRequests must be non-negative")
  require(lengthRequests >= 0, "lengthRequests must be non-negative")
  require(bytesRead >= 0L, "bytesRead must be non-negative")
  require(indexBytesRead >= 0L, "indexBytesRead must be non-negative")
  require(dataBytesRead >= 0L, "dataBytesRead must be non-negative")
  require(
    indexBytesRead + dataBytesRead == bytesRead,
    "index and data bytes must account for bytesRead"
  )
  require(touchedChunks >= 0, "touchedChunks must be non-negative")
  require(touchedShards >= 0, "touchedShards must be non-negative")
  require(requestedElements >= 0L, "requestedElements must be non-negative")
  require(elementByteWidth > 0, "elementByteWidth must be positive")
  require(
    requestedElements <= Long.MaxValue / elementByteWidth.toLong,
    "requested logical byte count must fit Long"
  )

  val requestedLogicalBytes: Long = requestedElements * elementByteWidth.toLong

  /** Physical encoded bytes fetched per requested logical scalar byte. */
  def readAmplification: Double =
    if requestedLogicalBytes == 0L then 0.0
    else bytesRead.toDouble / requestedLogicalBytes.toDouble

final case class ReadResult(block: PrimitiveBlock, shape: Shape, receipt: ExecutionReceipt)

object OpenValidation:
  def descriptor(
      descriptor: ArrayDescriptor,
      limits: OpenLimits
  ): Either[ZarrError, Unit] =
    if descriptor.shape.rank.toInt > limits.maxRank then
      Left(
        ZarrError.ResourceLimit(
          "array rank",
          limits.maxRank,
          descriptor.shape.rank.toInt
        )
      )
    else
      val chunkShape = descriptor.layout match
        case PhysicalLayout.Direct(_)                    => descriptor.grid.chunkShape
        case PhysicalLayout.Sharded(sharded, _, _, _, _) => sharded.innerChunkShape
      chunkShape.elementCount.flatMap: elements =>
        LongArrays
          .checkedMultiply(
            elements,
            descriptor.dataType.byteWidth.toLong,
            "decoded chunk byte length"
          )
          .flatMap: bytes =>
            if bytes > limits.maxDecodedChunkBytes.toLong then
              Left(
                ZarrError.ResourceLimit(
                  "decoded chunk bytes",
                  limits.maxDecodedChunkBytes.toLong,
                  bytes
                )
              )
            else Right(())

  def codecPrograms(
      descriptor: ArrayDescriptor,
      validate: CodecProgram => Either[ZarrError, Unit]
  ): Either[ZarrError, Unit] = descriptor.layout match
    case PhysicalLayout.Direct(codecs)                     => validate(codecs)
    case PhysicalLayout.Sharded(_, inner, index, _, outer) =>
      validate(inner).flatMap: _ =>
        validate(index.codecs).flatMap: _ =>
          validate(outer)

final class OpenedArray private[zarr4s] (
    store: ObjectReader,
    val path: ZarrPath,
    val descriptor: ArrayDescriptor,
    val format: ZarrFormat,
    runtime: SyncCodecRuntime
):
  def readRegion(
      region: Region,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, ReadResult] =
    read(ArraySelection.RegionSelection(region), region.extent, limits)

  def readPoints(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, ReadResult] =
    Shape(points.count.toLong).flatMap: outputShape =>
      read(ArraySelection.PointSelection(points), outputShape, limits)

  def read(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, ReadResult] =
    if selection.arrayShape != descriptor.shape then
      Left(ZarrError.InvalidSelection("factored selection belongs to a different array shape"))
    else
      PrimitiveBlockBuilder(
        descriptor.dataType,
        descriptor.fillValue,
        selection.outputShape
      ).flatMap: builder =>
        val folded = foldFragments(selection, builder, limits): (current, fragment) =>
          PrimitiveBlockBuilder
            .applyFragment(
              current,
              fragment,
              selection.outputShape
            )
            .map(_ => FragmentControl.Continue(current))
        folded.map: result =>
          val receipt = result.receipt
          ReadResult(
            result.state.result(),
            selection.outputShape,
            ExecutionReceipt(
              receipt.objectRequests,
              receipt.rangeRequests,
              receipt.lengthRequests,
              receipt.bytesRead,
              receipt.indexBytesRead,
              receipt.dataBytesRead,
              receipt.visitedChunks,
              receipt.plannedShards,
              receipt.requestedElements,
              receipt.elementByteWidth
            )
          )

  def foldFragments[S](
      selection: FactoredSelection,
      initial: S,
      limits: ReadLimits = ReadLimits()
  )(
      consume: (S, ChunkFragment) => Either[ZarrError, FragmentControl[S]]
  ): Either[ZarrError, FragmentFoldResult[S]] = descriptor.layout match
    case PhysicalLayout.Direct(codecs) =>
      foldDirectFragments(selection, initial, codecs, limits, consume)
    case PhysicalLayout.Sharded(sharded, innerCodecs, _, location, outerCodecs) =>
      if outerCodecs.nonEmpty then
        Left(
          ZarrError.UnsupportedRead(
            "outer codecs after sharding_indexed prevent bounded partial reads"
          )
        )
      else
        foldShardedFragments(
          selection,
          initial,
          sharded,
          innerCodecs,
          location,
          limits,
          consume
        )

  def foreachFragment(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  )(
      consume: ChunkFragment => Either[ZarrError, Unit]
  ): Either[ZarrError, FragmentReceipt] =
    val folded = foldFragments(selection, (), limits): (_, fragment) =>
      consume(fragment).map(_ => FragmentControl.Continue(()))
    folded.map(_.receipt)

  private def foldDirectFragments[S](
      selection: FactoredSelection,
      initial: S,
      codecs: CodecProgram,
      limits: ReadLimits,
      consume: (S, ChunkFragment) => Either[ZarrError, FragmentControl[S]]
  ): Either[ZarrError, FragmentFoldResult[S]] =
    val plan = ChunkPlanner.planFactored(descriptor.grid, selection, limits.planning) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    if plan.demands.length > limits.maxObjects then
      return Left(ZarrError.ResourceLimit("read objects", limits.maxObjects, plan.demands.length))
    val metrics = FragmentMetrics.checked(
      plan.demands.length,
      plan.stats.requestedElements,
      descriptor.dataType.byteWidth
    ) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    var state = initial
    var stopped = false
    var demandIndex = 0
    while demandIndex < plan.demands.length && !stopped do
      val demand = plan.demands(demandIndex)
      val copy = demand.copy match
        case ChunkCopy.FactoredCopy(found) => found
        case _                             =>
          return Left(ZarrError.InvalidSelection("fragment plan contains a non-factored copy"))
      val key = chunkStoreKey(demand.coordinate) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      metrics.objectRequests += 1
      val fragment = store.readAll(key, limits.maxEncodedObjectBytes) match
        case Left(StoreError.NotFound(_)) =>
          ChunkFragment.fill(
            demand.coordinate,
            copy,
            selection.outputShape,
            descriptor.dataType,
            descriptor.fillValue
          ) match
            case Left(error)  => return Left(error)
            case Right(found) =>
              metrics.fill(found)
              found
        case Left(error)    => return Left(ZarrError.StoreFailure(error))
        case Right(encoded) =>
          metrics.dataBytesRead += encoded.byteCount.toLong
          val chunkShape = ChunkGeometry.storedShape(descriptor.grid, demand.coordinate) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          val decoded = runtime.decode(
            encoded,
            codecs,
            descriptor.dataType,
            chunkShape,
            limits.decode
          ) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          ChunkFragment.decoded(
            demand.coordinate,
            decoded,
            chunkShape,
            copy,
            selection.outputShape,
            descriptor.dataType,
            descriptor.fillValue
          ) match
            case Left(error)  => return Left(error)
            case Right(found) =>
              metrics.decoded(found)
              found
      consume(state, fragment) match
        case Left(error)                            => return Left(error)
        case Right(FragmentControl.Continue(found)) => state = found
        case Right(FragmentControl.Stop(found))     =>
          state = found
          stopped = true
      demandIndex += 1
    Right(FragmentFoldResult(state, metrics.result(!stopped)))

  private def foldShardedFragments[S](
      selection: FactoredSelection,
      initial: S,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      limits: ReadLimits,
      consume: (S, ChunkFragment) => Either[ZarrError, FragmentControl[S]]
  ): Either[ZarrError, FragmentFoldResult[S]] =
    val innerPlan = ChunkPlanner.planFactored(
      sharded.globalInnerGrid,
      selection,
      limits.planning
    ) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    val grouped = ShardPlanner.group(sharded, innerPlan) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    if grouped.touchedShards > limits.maxObjects then
      return Left(ZarrError.ResourceLimit("read shards", limits.maxObjects, grouped.touchedShards))

    val metrics = FragmentMetrics.checked(
      innerPlan.demands.length,
      innerPlan.stats.requestedElements,
      descriptor.dataType.byteWidth,
      grouped.touchedShards
    ) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    var state = initial
    var stopped = false

    def emit(
        coordinate: ChunkCoordinate,
        copy: FactoredChunkCopy,
        decoded: Option[(PrimitiveBlock, Shape)]
    ): Either[ZarrError, Unit] =
      val fragment = decoded match
        case None =>
          ChunkFragment.fill(
            coordinate,
            copy,
            selection.outputShape,
            descriptor.dataType,
            descriptor.fillValue
          )
        case Some((block, shape)) =>
          ChunkFragment.decoded(
            coordinate,
            block,
            shape,
            copy,
            selection.outputShape,
            descriptor.dataType,
            descriptor.fillValue
          )
      fragment.flatMap: found =>
        if decoded.isEmpty then metrics.fill(found) else metrics.decoded(found)
        consume(state, found).map:
          case FragmentControl.Continue(next) => state = next
          case FragmentControl.Stop(next)     =>
            state = next
            stopped = true

    def checkedRangeRequest(): Either[ZarrError, Unit] =
      if metrics.rangeRequests >= limits.maxRanges then
        Left(ZarrError.ResourceLimit("read ranges", limits.maxRanges, metrics.rangeRequests + 1))
      else Right(())

    var shardIndex = 0
    while shardIndex < grouped.shards.length && !stopped do
      val shard = grouped.shards(shardIndex)
      val shardKey = chunkStoreKey(shard.coordinate) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      var missingShard = false
      var objectLength = Map.empty[Vector[Long], Long]
      if location == IndexLocation.End then
        metrics.objectRequests += 1
        metrics.lengthRequests += 1
        store.length(shardKey) match
          case Left(StoreError.NotFound(_)) => missingShard = true
          case Left(error)                  => return Left(ZarrError.StoreFailure(error))
          case Right(found) => objectLength = Map(shard.coordinate.toVector -> found)

      val decodedIndex =
        if missingShard then None
        else
          val single = ShardReadPlan(Vector(shard), shard.innerChunks.length, 1)
          val indexPlan = ShardIndexReadPlan(
            single,
            sharded.innerChunksPerShard,
            location,
            descriptor.chunkKeyEncoding,
            objectLength,
            limits.shardIndex
          ) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          val read = indexPlan.reads.head
          checkedRangeRequest() match
            case Left(error) => return Left(error)
            case Right(_)    => ()
          val key = path.key(read.key.value) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          metrics.objectRequests += 1
          metrics.rangeRequests += 1
          store.read(key, read.range) match
            case Left(StoreError.NotFound(_)) =>
              missingShard = true
              None
            case Left(error)  => return Left(ZarrError.StoreFailure(error))
            case Right(bytes) =>
              metrics.indexBytesRead += bytes.byteCount.toLong
              ShardIndexCodec.decode(bytes, sharded.innerChunksPerShard, limits.shardIndex) match
                case Left(error)  => return Left(error)
                case Right(found) => Some(found)

      var innerIndex = 0
      while innerIndex < shard.innerChunks.length && !stopped do
        val inner = shard.innerChunks(innerIndex)
        val copy = inner.copy match
          case ChunkCopy.FactoredCopy(found) => found
          case _                             =>
            return Left(ZarrError.InvalidSelection("fragment plan contains a non-factored copy"))
        val global = globalInnerCoordinate(shard.coordinate, inner.localCoordinate, sharded)
        if missingShard then
          emit(global, copy, None) match
            case Left(error) => return Left(error)
            case Right(_)    => ()
        else
          decodedIndex.get.entry(inner.localCoordinate) match
            case Left(error)                 => return Left(error)
            case Right(ShardIndexEntry.Fill) =>
              emit(global, copy, None) match
                case Left(error) => return Left(error)
                case Right(_)    => ()
            case Right(ShardIndexEntry.Present(offset, length)) =>
              if length.toLong > limits.maxEncodedObjectBytes.toLong then
                return Left(
                  ZarrError.ResourceLimit(
                    "encoded chunk bytes",
                    limits.maxEncodedObjectBytes.toLong,
                    length.toLong
                  )
                )
              checkedRangeRequest() match
                case Left(error) => return Left(error)
                case Right(_)    => ()
              val range = ByteRange(offset, length.toLong) match
                case Left(error)  => return Left(error)
                case Right(found) => found
              metrics.objectRequests += 1
              metrics.rangeRequests += 1
              store.read(shardKey, range) match
                case Left(StoreError.NotFound(_)) =>
                  missingShard = true
                  emit(global, copy, None) match
                    case Left(error) => return Left(error)
                    case Right(_)    => ()
                case Left(error)    => return Left(ZarrError.StoreFailure(error))
                case Right(encoded) =>
                  metrics.dataBytesRead += encoded.byteCount.toLong
                  val chunkShape = ChunkGeometry.storedShape(sharded.globalInnerGrid, global) match
                    case Left(error)  => return Left(error)
                    case Right(found) => found
                  val decoded = runtime.decode(
                    encoded,
                    innerCodecs,
                    descriptor.dataType,
                    chunkShape,
                    limits.decode
                  ) match
                    case Left(error)  => return Left(error)
                    case Right(found) => found
                  emit(global, copy, Some(decoded -> chunkShape)) match
                    case Left(error) => return Left(error)
                    case Right(_)    => ()
        innerIndex += 1
      shardIndex += 1
    Right(FragmentFoldResult(state, metrics.result(!stopped)))

  private def read(
      selection: ArraySelection,
      outputShape: Shape,
      limits: ReadLimits
  ): Either[ZarrError, ReadResult] =
    PrimitiveBlockBuilder(descriptor.dataType, descriptor.fillValue, outputShape).flatMap:
      builder =>
        descriptor.layout match
          case PhysicalLayout.Direct(codecs) =>
            readDirect(selection, outputShape, builder, codecs, limits)
          case PhysicalLayout.Sharded(sharded, innerCodecs, _, location, outerCodecs) =>
            if outerCodecs.nonEmpty then
              Left(
                ZarrError.UnsupportedRead(
                  "outer codecs after sharding_indexed prevent bounded partial reads"
                )
              )
            else
              readSharded(
                selection,
                outputShape,
                builder,
                sharded,
                innerCodecs,
                location,
                limits
              )

  private def readDirect(
      selection: ArraySelection,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      codecs: CodecProgram,
      limits: ReadLimits
  ): Either[ZarrError, ReadResult] =
    ChunkPlanner.plan(descriptor.grid, selection, limits.planning) match
      case Left(error) => Left(error)
      case Right(plan) =>
        if plan.demands.length > limits.maxObjects then
          return Left(
            ZarrError.ResourceLimit("read objects", limits.maxObjects, plan.demands.length)
          )
        var objectRequests = 0
        var bytesRead = 0L
        var demandIndex = 0
        while demandIndex < plan.demands.length do
          val demand = plan.demands(demandIndex)
          val key = chunkStoreKey(demand.coordinate) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          objectRequests += 1
          store.readAll(key, limits.maxEncodedObjectBytes) match
            case Left(StoreError.NotFound(_)) => ()
            case Left(error)                  => return Left(ZarrError.StoreFailure(error))
            case Right(encoded)               =>
              bytesRead += encoded.byteCount.toLong
              val chunkShape = ChunkGeometry.storedShape(descriptor.grid, demand.coordinate) match
                case Left(error)  => return Left(error)
                case Right(found) => found
              val decoded = runtime.decode(
                encoded,
                codecs,
                descriptor.dataType,
                chunkShape,
                limits.decode
              ) match
                case Left(error)  => return Left(error)
                case Right(found) => found
              PrimitiveBlockBuilder.applyCopy(
                builder,
                decoded,
                chunkShape,
                outputShape,
                demand.copy
              ) match
                case Left(error) => return Left(error)
                case Right(_)    => ()
          demandIndex += 1
        Right(
          ReadResult(
            builder.result(),
            outputShape,
            ExecutionReceipt(
              objectRequests,
              0,
              0,
              bytesRead,
              0L,
              bytesRead,
              plan.demands.length,
              0,
              plan.stats.requestedElements,
              descriptor.dataType.byteWidth
            )
          )
        )

  private def readSharded(
      selection: ArraySelection,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      limits: ReadLimits
  ): Either[ZarrError, ReadResult] =
    for
      innerPlan <- ChunkPlanner.plan(sharded.globalInnerGrid, selection, limits.planning)
      grouped <- ShardPlanner.group(sharded, innerPlan)
      result <- executeShards(
        grouped,
        outputShape,
        builder,
        sharded,
        innerCodecs,
        location,
        limits,
        innerPlan.stats.requestedElements
      )
    yield result

  private def executeShards(
      grouped: ShardReadPlan,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      limits: ReadLimits,
      requestedElements: Long
  ): Either[ZarrError, ReadResult] =
    if grouped.touchedShards > limits.maxObjects then
      Left(ZarrError.ResourceLimit("read shards", limits.maxObjects, grouped.touchedShards))
    else
      val objectLengths = scala.collection.mutable.Map.empty[Vector[Long], Long]
      val missing = scala.collection.mutable.HashSet.empty[Vector[Long]]
      var objectRequests = 0
      var rangeRequests = 0
      var bytesRead = 0L
      var indexBytesRead = 0L
      if location == IndexLocation.End then
        var shardIndex = 0
        while shardIndex < grouped.shards.length do
          val shard = grouped.shards(shardIndex)
          val key = chunkStoreKey(shard.coordinate) match
            case Left(error)  => return Left(error)
            case Right(found) => found
          objectRequests += 1
          store.length(key) match
            case Left(StoreError.NotFound(_)) => missing += shard.coordinate.toVector
            case Left(error)                  => return Left(ZarrError.StoreFailure(error))
            case Right(length)                =>
              objectLengths.update(shard.coordinate.toVector, length)
          shardIndex += 1

      val presentShards =
        grouped.shards.filterNot(shard => missing.contains(shard.coordinate.toVector))
      val presentPlan = ShardReadPlan(
        presentShards,
        presentShards.map(_.innerChunks.length).sum,
        presentShards.length
      )
      val indexPlan = ShardIndexReadPlan(
        presentPlan,
        sharded.innerChunksPerShard,
        location,
        descriptor.chunkKeyEncoding,
        objectLengths.toMap,
        limits.shardIndex
      ) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      val indexes = scala.collection.mutable.Map.empty[Vector[Long], ShardIndex]
      var readIndex = 0
      while readIndex < indexPlan.reads.length do
        val read = indexPlan.reads(readIndex)
        val key = path.key(read.key.value) match
          case Left(error)  => return Left(error)
          case Right(found) => found
        objectRequests += 1
        rangeRequests += 1
        store.read(key, read.range) match
          case Left(StoreError.NotFound(_)) => missing += read.shardCoordinate.toVector
          case Left(error)                  => return Left(ZarrError.StoreFailure(error))
          case Right(bytes)                 =>
            bytesRead += bytes.byteCount.toLong
            indexBytesRead += bytes.byteCount.toLong
            ShardIndexCodec.decode(bytes, sharded.innerChunksPerShard, limits.shardIndex) match
              case Left(error)  => return Left(error)
              case Right(index) => indexes.update(read.shardCoordinate.toVector, index)
        readIndex += 1

      val fillEntries = sharded.innerChunksPerShard.elementCount match
        case Left(error)  => return Left(error)
        case Right(count) => Vector.fill(count.toInt)(ShardIndexEntry.Fill)
      val fillIndex = ShardIndex(sharded.innerChunksPerShard, fillEntries, limits.shardIndex) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      grouped.shards.foreach: shard =>
        if missing.contains(shard.coordinate.toVector) then
          indexes.update(shard.coordinate.toVector, fillIndex)

      val dataPlan = ShardDataPlan.resolve(
        grouped,
        indexes.toMap,
        descriptor.chunkKeyEncoding
      ) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      if dataPlan.rangeReads > limits.maxRanges then
        return Left(ZarrError.ResourceLimit("read ranges", limits.maxRanges, dataPlan.rangeReads))

      var dataShardIndex = 0
      while dataShardIndex < dataPlan.shards.length do
        val shard = dataPlan.shards(dataShardIndex)
        val ranged = shard.chunks.collect:
          case chunk @ ShardChunkRead(_, ShardChunkSource.Range(range), _) => range -> chunk
        val coalesced = RangeCoalescer.coalesce(ranged, limits.coalescing) match
          case Left(error)  => return Left(error)
          case Right(found) => found
        val key = path.key(shard.key.value) match
          case Left(error)  => return Left(error)
          case Right(found) => found
        var coalescedIndex = 0
        while coalescedIndex < coalesced.length do
          val read = coalesced(coalescedIndex)
          objectRequests += 1
          rangeRequests += 1
          store.read(key, read.range) match
            case Left(StoreError.NotFound(_)) => ()
            case Left(error)                  => return Left(ZarrError.StoreFailure(error))
            case Right(bytes)                 =>
              bytesRead += bytes.byteCount.toLong
              var memberIndex = 0
              while memberIndex < read.members.length do
                val member = read.members(memberIndex)
                val encoded =
                  bytes.slice(member.relativeOffset, member.relativeOffset + member.length)
                val global = globalInnerCoordinate(
                  shard.coordinate,
                  member.value.localCoordinate,
                  sharded
                )
                val chunkShape = ChunkGeometry.storedShape(sharded.globalInnerGrid, global) match
                  case Left(error)  => return Left(error)
                  case Right(found) => found
                val decoded = runtime.decode(
                  encoded,
                  innerCodecs,
                  descriptor.dataType,
                  chunkShape,
                  limits.decode
                ) match
                  case Left(error)  => return Left(error)
                  case Right(found) => found
                PrimitiveBlockBuilder.applyCopy(
                  builder,
                  decoded,
                  chunkShape,
                  outputShape,
                  member.value.copy
                ) match
                  case Left(error) => return Left(error)
                  case Right(_)    => ()
                memberIndex += 1
          coalescedIndex += 1
        dataShardIndex += 1
      Right(
        ReadResult(
          builder.result(),
          outputShape,
          ExecutionReceipt(
            objectRequests,
            rangeRequests,
            if location == IndexLocation.End then grouped.shards.length else 0,
            bytesRead,
            indexBytesRead,
            bytesRead - indexBytesRead,
            grouped.touchedInnerChunks,
            grouped.touchedShards,
            requestedElements,
            descriptor.dataType.byteWidth
          )
        )
      )

  private def globalInnerCoordinate(
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

  private def chunkStoreKey(coordinate: ChunkCoordinate): Either[ZarrError, StoreKey] =
    path.key(descriptor.chunkKeyEncoding.encode(coordinate).value)

enum OpenedNode:
  case Group(value: OpenedGroup)
  case Array(value: OpenedArray)

final class OpenedGroup private[zarr4s] (
    store: ObjectReader,
    val path: ZarrPath,
    val metadata: GroupMetadata,
    val format: ZarrFormat,
    index: Option[HierarchyIndex],
    capabilities: ZarrCapabilities,
    limits: OpenLimits,
    runtime: SyncCodecRuntime,
    consolidation: ConsolidationMode
):
  def children: Either[ZarrError, Vector[HierarchyEntry]] = index match
    case Some(found) => Right(found.children(path))
    case None        =>
      Left(
        ZarrError.UnsupportedRead(
          "hierarchy discovery requires consolidated metadata or a listing capability"
        )
      )

  def open(relativePath: String): Either[ZarrError, OpenedNode] =
    path
      .resolve(relativePath)
      .flatMap: resolved =>
        index.flatMap(_.document(resolved)) match
          case Some(document) =>
            SyncZarr.openDocument(
              store,
              resolved,
              document,
              index,
              capabilities,
              limits,
              runtime,
              consolidation
            )
          case None if consolidation == ConsolidationMode.Require =>
            Left(
              ZarrError.InvalidMetadata(
                "$.consolidated_metadata",
                s"required metadata does not contain '${resolved.value}'"
              )
            )
          case None =>
            SyncZarr.openNode(
              store,
              resolved,
              capabilities,
              limits,
              runtime,
              ConsolidationMode.Ignore
            )

  def openArray(relativePath: String): Either[ZarrError, OpenedArray] =
    open(relativePath).flatMap:
      case OpenedNode.Array(found) => Right(found)
      case OpenedNode.Group(_)     => Left(ZarrError.UnsupportedNodeType("group"))

  def openGroup(relativePath: String): Either[ZarrError, OpenedGroup] =
    open(relativePath).flatMap:
      case OpenedNode.Group(found) => Right(found)
      case OpenedNode.Array(_)     => Left(ZarrError.UnsupportedNodeType("array"))

object SyncZarr:
  def openArray(
      store: ObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedArray] =
    openNode(store, path, capabilities, limits, runtime, consolidation).flatMap:
      case OpenedNode.Array(found) => Right(found)
      case OpenedNode.Group(_)     => Left(ZarrError.UnsupportedNodeType("group"))

  def openGroup(
      store: ObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedGroup] =
    openNode(store, path, capabilities, limits, runtime, consolidation).flatMap:
      case OpenedNode.Group(found) => Right(found)
      case OpenedNode.Array(_)     => Left(ZarrError.UnsupportedNodeType("array"))

  def openNode(
      store: ObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedNode] =
    readOptional(store, path, "zarr.json", limits.maxMetadataBytes).flatMap:
      case Some(json) =>
        ZarrMetadata
          .parse(json)
          .flatMap:
            case ZarrNodeMetadata.Group(group) =>
              val indexed = consolidation match
                case ConsolidationMode.Ignore => Right(None)
                case _                        =>
                  HierarchyIndex
                    .v3(path, group, limits.hierarchy)
                    .flatMap:
                      case None if consolidation == ConsolidationMode.Require =>
                        Left(
                          ZarrError.InvalidMetadata(
                            "$.consolidated_metadata",
                            "required consolidated metadata is absent"
                          )
                        )
                      case found => Right(found)
              indexed.map: found =>
                OpenedNode.Group(
                  new OpenedGroup(
                    store,
                    path,
                    group,
                    ZarrFormat.V3,
                    found,
                    capabilities,
                    limits,
                    runtime,
                    consolidation
                  )
                )
            case ZarrNodeMetadata.Array(array) =>
              openDocument(
                store,
                path,
                HierarchyDocument.V3Array(array),
                None,
                capabilities,
                limits,
                runtime,
                consolidation
              )
      case None => openV2(store, path, capabilities, limits, runtime, consolidation)

  private[zarr4s] def openDocument(
      store: ObjectReader,
      path: ZarrPath,
      document: HierarchyDocument,
      index: Option[HierarchyIndex],
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: SyncCodecRuntime,
      consolidation: ConsolidationMode
  ): Either[ZarrError, OpenedNode] = document.kind match
    case NodeKind.Group =>
      document.groupMetadata.map: group =>
        OpenedNode.Group(
          new OpenedGroup(
            store,
            path,
            group,
            document.format,
            index,
            capabilities,
            limits,
            runtime,
            consolidation
          )
        )
    case NodeKind.Array =>
      document
        .arrayDescriptor(capabilities)
        .flatMap: descriptor =>
          OpenValidation
            .descriptor(descriptor, limits)
            .flatMap: _ =>
              OpenValidation
                .codecPrograms(descriptor, runtime.validate)
                .map: _ =>
                  OpenedNode.Array(
                    new OpenedArray(store, path, descriptor, document.format, runtime)
                  )

  private def openV2(
      store: ObjectReader,
      path: ZarrPath,
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: SyncCodecRuntime,
      consolidation: ConsolidationMode
  ): Either[ZarrError, OpenedNode] =
    val indexed = consolidation match
      case ConsolidationMode.Ignore => Right(None)
      case _                        =>
        readOptional(store, ZarrPath.root, ".zmetadata", limits.maxMetadataBytes).flatMap:
          case Some(json) =>
            HierarchyIndex.v2(ZarrPath.root, json, limits.hierarchy).map(Some.apply)
          case None if consolidation == ConsolidationMode.Require =>
            Left(
              ZarrError.InvalidMetadata(
                "$.zmetadata",
                "required consolidated metadata is absent"
              )
            )
          case None => Right(None)
    indexed.flatMap: found =>
      found.flatMap(_.document(path)) match
        case Some(document) =>
          openDocument(
            store,
            path,
            document,
            found,
            capabilities,
            limits,
            runtime,
            consolidation
          )
        case None if consolidation == ConsolidationMode.Require =>
          Left(
            ZarrError.InvalidMetadata(
              "$.zmetadata.metadata",
              s"required metadata does not contain '${path.value}'"
            )
          )
        case None => openV2Individual(store, path, capabilities, limits, runtime)

  private def openV2Individual(
      store: ObjectReader,
      path: ZarrPath,
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: SyncCodecRuntime
  ): Either[ZarrError, OpenedNode] =
    readOptional(store, path, ".zarray", limits.maxMetadataBytes).flatMap:
      case Some(arrayJson) =>
        readOptional(store, path, ".zattrs", limits.maxMetadataBytes).flatMap: attributes =>
          V2Metadata
            .parseArray(arrayJson, attributes)
            .flatMap: array =>
              openDocument(
                store,
                path,
                HierarchyDocument.V2Array(array),
                None,
                capabilities,
                limits,
                runtime,
                ConsolidationMode.Ignore
              )
      case None =>
        readOptional(store, path, ".zgroup", limits.maxMetadataBytes).flatMap:
          case Some(groupJson) =>
            readOptional(store, path, ".zattrs", limits.maxMetadataBytes).flatMap: attributes =>
              V2Metadata
                .parseGroup(groupJson, attributes)
                .map: group =>
                  OpenedNode.Group(
                    new OpenedGroup(
                      store,
                      path,
                      group,
                      ZarrFormat.V2,
                      None,
                      capabilities,
                      limits,
                      runtime,
                      ConsolidationMode.Ignore
                    )
                  )
          case None =>
            path
              .key(".zarray")
              .flatMap: key =>
                Left(ZarrError.StoreFailure(StoreError.NotFound(key)))

  private def readOptional(
      store: ObjectReader,
      path: ZarrPath,
      child: String,
      limit: ByteCount
  ): Either[ZarrError, Option[String]] = path
    .key(child)
    .flatMap: key =>
      store.readAll(key, limit) match
        case Left(StoreError.NotFound(_)) => Right(None)
        case Left(error)                  => Left(ZarrError.StoreFailure(error))
        case Right(bytes)                 => Right(Some(new String(bytes.values, "UTF-8")))
