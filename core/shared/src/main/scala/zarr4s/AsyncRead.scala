package zarr4s

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

private[zarr4s] object AsyncBatch:
  def traverseBounded[A, B](
      values: Vector[A],
      parallelism: Int
  )(operation: A => Future[B])(using ExecutionContext): Future[Vector[B]] =
    if values.isEmpty then Future.successful(Vector.empty)
    else
      val results = new Array[Any](values.length)
      var next = 0
      val gate = new AnyRef

      def claim(): Int = gate.synchronized:
        if next >= values.length then -1
        else
          val index = next
          next += 1
          index

      def worker(): Future[Unit] =
        val index = claim()
        if index < 0 then Future.successful(())
        else
          operation(values(index)).flatMap: result =>
            results(index) = result.asInstanceOf[Any]
            worker()

      val workers = Vector.fill(math.min(parallelism, values.length))(worker())
      Future
        .sequence(workers)
        .map: _ =>
          Vector.tabulate(values.length)(index => results(index).asInstanceOf[B])

final class AsyncOpenedArray private[zarr4s] (
    store: AsyncObjectReader,
    val path: ZarrPath,
    val descriptor: ArrayDescriptor,
    val format: ZarrFormat,
    runtime: AsyncCodecRuntime
)(using ExecutionContext):
  def readRegion(
      region: Region,
      limits: ReadLimits = ReadLimits()
  ): Future[Either[ZarrError, ReadResult]] =
    read(ArraySelection.RegionSelection(region), region.extent, limits)

  def readPoints(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  ): Future[Either[ZarrError, ReadResult]] = Shape(points.count.toLong) match
    case Left(error)        => Future.successful(Left(error))
    case Right(outputShape) => read(ArraySelection.PointSelection(points), outputShape, limits)

  def read(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  ): Future[Either[ZarrError, ReadResult]] =
    if selection.arrayShape != descriptor.shape then
      Future.successful(
        Left(
          ZarrError.InvalidSelection(
            "factored selection belongs to a different array shape"
          )
        )
      )
    else
      PrimitiveBlockBuilder(
        descriptor.dataType,
        descriptor.fillValue,
        selection.outputShape
      ) match
        case Left(error)    => Future.successful(Left(error))
        case Right(builder) =>
          val folded = foldFragments(selection, builder, limits): (current, fragment) =>
            Future.successful(
              PrimitiveBlockBuilder
                .applyFragment(
                  current,
                  fragment,
                  selection.outputShape
                )
                .map(_ => FragmentControl.Continue(current))
            )
          folded.map(_.map: result =>
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
            ))

  def foldFragments[S](
      selection: FactoredSelection,
      initial: S,
      limits: ReadLimits = ReadLimits()
  )(
      consume: (S, ChunkFragment) => Future[Either[ZarrError, FragmentControl[S]]]
  ): Future[Either[ZarrError, FragmentFoldResult[S]]] = descriptor.layout match
    case PhysicalLayout.Direct(codecs) =>
      foldDirectFragments(selection, initial, codecs, limits, consume)
    case PhysicalLayout.Sharded(sharded, innerCodecs, _, location, outerCodecs) =>
      if outerCodecs.nonEmpty then
        Future.successful(
          Left(
            ZarrError.UnsupportedRead(
              "outer codecs after sharding_indexed prevent bounded partial reads"
            )
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
      consume: ChunkFragment => Future[Either[ZarrError, Unit]]
  ): Future[Either[ZarrError, FragmentReceipt]] =
    val folded = foldFragments(selection, (), limits): (_, fragment) =>
      consume(fragment).map(_.map(_ => FragmentControl.Continue(())))
    folded.map(_.map(_.receipt))

  private final case class FragmentProgress[S](state: S, stopped: Boolean)

  private def deliver[S](
      state: S,
      fragment: Either[ZarrError, ChunkFragment],
      metrics: FragmentMetrics,
      consume: (S, ChunkFragment) => Future[Either[ZarrError, FragmentControl[S]]]
  ): Future[Either[ZarrError, FragmentProgress[S]]] = fragment match
    case Left(error)  => Future.successful(Left(error))
    case Right(found) =>
      found.source match
        case FragmentSource.Decoded => metrics.decoded(found)
        case FragmentSource.Fill    => metrics.fill(found)
      consume(state, found).map(_.map:
        case FragmentControl.Continue(next) => FragmentProgress(next, stopped = false)
        case FragmentControl.Stop(next)     => FragmentProgress(next, stopped = true))

  private def foldDirectFragments[S](
      selection: FactoredSelection,
      initial: S,
      codecs: CodecProgram,
      limits: ReadLimits,
      consume: (S, ChunkFragment) => Future[Either[ZarrError, FragmentControl[S]]]
  ): Future[Either[ZarrError, FragmentFoldResult[S]]] =
    ChunkPlanner.planFactored(descriptor.grid, selection, limits.planning) match
      case Left(error)                                            => Future.successful(Left(error))
      case Right(plan) if plan.demands.length > limits.maxObjects =>
        Future.successful(
          Left(
            ZarrError.ResourceLimit(
              "read objects",
              limits.maxObjects,
              plan.demands.length
            )
          )
        )
      case Right(plan) =>
        val metrics = FragmentMetrics.checked(
          plan.demands.length,
          plan.stats.requestedElements,
          descriptor.dataType.byteWidth
        ) match
          case Left(error)  => return Future.successful(Left(error))
          case Right(found) => found

        def loop(index: Int, state: S): Future[Either[ZarrError, FragmentFoldResult[S]]] =
          if index >= plan.demands.length then
            Future.successful(Right(FragmentFoldResult(state, metrics.result(completed = true))))
          else
            val demand = plan.demands(index)
            demand.copy match
              case ChunkCopy.FactoredCopy(copy) =>
                chunkStoreKey(demand.coordinate) match
                  case Left(error) => Future.successful(Left(error))
                  case Right(key)  =>
                    metrics.objectRequests += 1
                    store
                      .readAll(key, limits.maxEncodedObjectBytes)
                      .flatMap:
                        case Left(StoreError.NotFound(_)) =>
                          deliver(
                            state,
                            ChunkFragment.fill(
                              demand.coordinate,
                              copy,
                              selection.outputShape,
                              descriptor.dataType,
                              descriptor.fillValue
                            ),
                            metrics,
                            consume
                          ).flatMap(continueDirect(index, _, loop, metrics))
                        case Left(error) => Future.successful(Left(ZarrError.StoreFailure(error)))
                        case Right(encoded) =>
                          metrics.dataBytesRead += encoded.byteCount.toLong
                          ChunkGeometry.storedShape(descriptor.grid, demand.coordinate) match
                            case Left(error)       => Future.successful(Left(error))
                            case Right(chunkShape) =>
                              runtime
                                .decode(
                                  encoded,
                                  codecs,
                                  descriptor.dataType,
                                  chunkShape,
                                  limits.decode
                                )
                                .flatMap:
                                  case Left(error)    => Future.successful(Left(error))
                                  case Right(decoded) =>
                                    deliver(
                                      state,
                                      ChunkFragment.decoded(
                                        demand.coordinate,
                                        decoded,
                                        chunkShape,
                                        copy,
                                        selection.outputShape,
                                        descriptor.dataType,
                                        descriptor.fillValue
                                      ),
                                      metrics,
                                      consume
                                    ).flatMap(continueDirect(index, _, loop, metrics))
              case _ =>
                Future.successful(
                  Left(
                    ZarrError.InvalidSelection(
                      "fragment plan contains a non-factored copy"
                    )
                  )
                )
        loop(0, initial)

  private def continueDirect[S](
      index: Int,
      progress: Either[ZarrError, FragmentProgress[S]],
      loop: (Int, S) => Future[Either[ZarrError, FragmentFoldResult[S]]],
      metrics: FragmentMetrics
  ): Future[Either[ZarrError, FragmentFoldResult[S]]] = progress match
    case Left(error)                   => Future.successful(Left(error))
    case Right(found) if found.stopped =>
      Future.successful(
        Right(
          FragmentFoldResult(
            found.state,
            metrics.result(completed = false)
          )
        )
      )
    case Right(found) => loop(index + 1, found.state)

  private def foldShardedFragments[S](
      selection: FactoredSelection,
      initial: S,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      limits: ReadLimits,
      consume: (S, ChunkFragment) => Future[Either[ZarrError, FragmentControl[S]]]
  ): Future[Either[ZarrError, FragmentFoldResult[S]]] =
    val planned = for
      inner <- ChunkPlanner.planFactored(sharded.globalInnerGrid, selection, limits.planning)
      grouped <- ShardPlanner.group(sharded, inner)
    yield inner -> grouped
    planned match
      case Left(error) => Future.successful(Left(error))
      case Right((_, grouped)) if grouped.touchedShards > limits.maxObjects =>
        Future.successful(
          Left(
            ZarrError.ResourceLimit(
              "read shards",
              limits.maxObjects,
              grouped.touchedShards
            )
          )
        )
      case Right((innerPlan, grouped)) =>
        val metrics = FragmentMetrics.checked(
          innerPlan.demands.length,
          innerPlan.stats.requestedElements,
          descriptor.dataType.byteWidth,
          grouped.touchedShards
        ) match
          case Left(error)  => return Future.successful(Left(error))
          case Right(found) => found

        def loopShards(
            index: Int,
            state: S
        ): Future[Either[ZarrError, FragmentFoldResult[S]]] =
          if index >= grouped.shards.length then
            Future.successful(Right(FragmentFoldResult(state, metrics.result(completed = true))))
          else
            val shard = grouped.shards(index)
            chunkStoreKey(shard.coordinate) match
              case Left(error)     => Future.successful(Left(error))
              case Right(shardKey) =>
                fetchShardIndex(
                  shard,
                  sharded,
                  location,
                  limits,
                  metrics
                ).flatMap:
                  case Left(error)         => Future.successful(Left(error))
                  case Right(decodedIndex) =>
                    processShardFragments(
                      shard,
                      shardKey,
                      decodedIndex,
                      selection,
                      sharded,
                      innerCodecs,
                      state,
                      limits,
                      metrics,
                      consume
                    ).flatMap:
                      case Left(error)                   => Future.successful(Left(error))
                      case Right(found) if found.stopped =>
                        Future.successful(
                          Right(
                            FragmentFoldResult(
                              found.state,
                              metrics.result(completed = false)
                            )
                          )
                        )
                      case Right(found) => loopShards(index + 1, found.state)
        loopShards(0, initial)

  private def fetchShardIndex(
      shard: ShardDemand,
      sharded: ShardedGrid,
      location: IndexLocation,
      limits: ReadLimits,
      metrics: FragmentMetrics
  ): Future[Either[ZarrError, Option[ShardIndex]]] =
    def fetch(objectLengths: Map[Vector[Long], Long]) =
      val single = ShardReadPlan(Vector(shard), shard.innerChunks.length, 1)
      ShardIndexReadPlan(
        single,
        sharded.innerChunksPerShard,
        location,
        descriptor.chunkKeyEncoding,
        objectLengths,
        limits.shardIndex
      ) match
        case Left(error) => Future.successful(Left(error))
        case Right(plan) =>
          val read = plan.reads.head
          checkedRangeRequest(metrics, limits) match
            case Left(error) => Future.successful(Left(error))
            case Right(_)    =>
              path.key(read.key.value) match
                case Left(error) => Future.successful(Left(error))
                case Right(key)  =>
                  metrics.objectRequests += 1
                  metrics.rangeRequests += 1
                  store
                    .read(key, read.range)
                    .map:
                      case Left(StoreError.NotFound(_)) => Right(None)
                      case Left(error)                  => Left(ZarrError.StoreFailure(error))
                      case Right(bytes)                 =>
                        metrics.indexBytesRead += bytes.byteCount.toLong
                        ShardIndexCodec
                          .decode(
                            bytes,
                            sharded.innerChunksPerShard,
                            limits.shardIndex
                          )
                          .map(Some.apply)

    if location == IndexLocation.Start then fetch(Map.empty)
    else
      chunkStoreKey(shard.coordinate) match
        case Left(error) => Future.successful(Left(error))
        case Right(key)  =>
          metrics.objectRequests += 1
          metrics.lengthRequests += 1
          store
            .length(key)
            .flatMap:
              case Left(StoreError.NotFound(_)) => Future.successful(Right(None))
              case Left(error)   => Future.successful(Left(ZarrError.StoreFailure(error)))
              case Right(length) => fetch(Map(shard.coordinate.toVector -> length))

  private def processShardFragments[S](
      shard: ShardDemand,
      shardKey: StoreKey,
      decodedIndex: Option[ShardIndex],
      selection: FactoredSelection,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      initial: S,
      limits: ReadLimits,
      metrics: FragmentMetrics,
      consume: (S, ChunkFragment) => Future[Either[ZarrError, FragmentControl[S]]]
  ): Future[Either[ZarrError, FragmentProgress[S]]] =
    def loop(
        index: Int,
        state: S,
        missingShard: Boolean
    ): Future[Either[ZarrError, FragmentProgress[S]]] =
      if index >= shard.innerChunks.length then
        Future.successful(Right(FragmentProgress(state, stopped = false)))
      else
        val inner = shard.innerChunks(index)
        inner.copy match
          case ChunkCopy.FactoredCopy(copy) =>
            val global = globalInnerCoordinate(shard.coordinate, inner.localCoordinate, sharded)

            def fill(nextMissing: Boolean) = deliver(
              state,
              ChunkFragment.fill(
                global,
                copy,
                selection.outputShape,
                descriptor.dataType,
                descriptor.fillValue
              ),
              metrics,
              consume
            ).flatMap:
              case Left(error)                   => Future.successful(Left(error))
              case Right(found) if found.stopped => Future.successful(Right(found))
              case Right(found)                  => loop(index + 1, found.state, nextMissing)

            if missingShard || decodedIndex.isEmpty then fill(nextMissing = true)
            else
              decodedIndex.get.entry(inner.localCoordinate) match
                case Left(error)                 => Future.successful(Left(error))
                case Right(ShardIndexEntry.Fill) => fill(nextMissing = false)
                case Right(ShardIndexEntry.Present(offset, length)) =>
                  if length.toLong > limits.maxEncodedObjectBytes.toLong then
                    Future.successful(
                      Left(
                        ZarrError.ResourceLimit(
                          "encoded chunk bytes",
                          limits.maxEncodedObjectBytes.toLong,
                          length.toLong
                        )
                      )
                    )
                  else
                    checkedRangeRequest(metrics, limits) match
                      case Left(error) => Future.successful(Left(error))
                      case Right(_)    =>
                        ByteRange(offset, length.toLong) match
                          case Left(error)  => Future.successful(Left(error))
                          case Right(range) =>
                            metrics.objectRequests += 1
                            metrics.rangeRequests += 1
                            store
                              .read(shardKey, range)
                              .flatMap:
                                case Left(StoreError.NotFound(_)) => fill(nextMissing = true)
                                case Left(error)                  =>
                                  Future.successful(Left(ZarrError.StoreFailure(error)))
                                case Right(encoded) =>
                                  metrics.dataBytesRead += encoded.byteCount.toLong
                                  ChunkGeometry.storedShape(sharded.globalInnerGrid, global) match
                                    case Left(error)       => Future.successful(Left(error))
                                    case Right(chunkShape) =>
                                      runtime
                                        .decode(
                                          encoded,
                                          innerCodecs,
                                          descriptor.dataType,
                                          chunkShape,
                                          limits.decode
                                        )
                                        .flatMap:
                                          case Left(error)    => Future.successful(Left(error))
                                          case Right(decoded) =>
                                            deliver(
                                              state,
                                              ChunkFragment.decoded(
                                                global,
                                                decoded,
                                                chunkShape,
                                                copy,
                                                selection.outputShape,
                                                descriptor.dataType,
                                                descriptor.fillValue
                                              ),
                                              metrics,
                                              consume
                                            ).flatMap:
                                              case Left(error) => Future.successful(Left(error))
                                              case Right(found) if found.stopped =>
                                                Future.successful(Right(found))
                                              case Right(found) =>
                                                loop(index + 1, found.state, missingShard = false)
          case _ =>
            Future.successful(
              Left(
                ZarrError.InvalidSelection(
                  "fragment plan contains a non-factored copy"
                )
              )
            )
    loop(0, initial, decodedIndex.isEmpty)

  private def checkedRangeRequest(
      metrics: FragmentMetrics,
      limits: ReadLimits
  ): Either[ZarrError, Unit] =
    if metrics.rangeRequests >= limits.maxRanges then
      Left(ZarrError.ResourceLimit("read ranges", limits.maxRanges, metrics.rangeRequests + 1))
    else Right(())

  private def read(
      selection: ArraySelection,
      outputShape: Shape,
      limits: ReadLimits
  ): Future[Either[ZarrError, ReadResult]] =
    PrimitiveBlockBuilder(descriptor.dataType, descriptor.fillValue, outputShape) match
      case Left(error)    => Future.successful(Left(error))
      case Right(builder) =>
        descriptor.layout match
          case PhysicalLayout.Direct(codecs) =>
            readDirect(selection, outputShape, builder, codecs, limits)
          case PhysicalLayout.Sharded(sharded, innerCodecs, _, location, outerCodecs) =>
            if outerCodecs.nonEmpty then
              Future.successful(
                Left(
                  ZarrError.UnsupportedRead(
                    "outer codecs after sharding_indexed prevent bounded partial reads"
                  )
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

  private final case class DirectFetch(
      demand: ChunkDemand,
      encoded: Option[OwnedBytes]
  )

  private final case class DecodedCopy(
      copy: ChunkCopy,
      block: PrimitiveBlock,
      chunkShape: Shape
  )

  private def readDirect(
      selection: ArraySelection,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      codecs: CodecProgram,
      limits: ReadLimits
  ): Future[Either[ZarrError, ReadResult]] = ChunkPlanner.plan(
    descriptor.grid,
    selection,
    limits.planning
  ) match
    case Left(error)                                            => Future.successful(Left(error))
    case Right(plan) if plan.demands.length > limits.maxObjects =>
      Future.successful(
        Left(
          ZarrError.ResourceLimit(
            "read objects",
            limits.maxObjects,
            plan.demands.length
          )
        )
      )
    case Right(plan) =>
      AsyncBatch
        .traverseBounded(plan.demands, limits.maxConcurrentRequests): demand =>
          chunkStoreKey(demand.coordinate) match
            case Left(error) => Future.successful(Left(error))
            case Right(key)  =>
              store
                .readAll(key, limits.maxEncodedObjectBytes)
                .map:
                  case Left(StoreError.NotFound(_)) => Right(DirectFetch(demand, None))
                  case Left(error)                  => Left(ZarrError.StoreFailure(error))
                  case Right(bytes)                 => Right(DirectFetch(demand, Some(bytes)))
        .flatMap: fetched =>
          firstError(fetched) match
            case Some(error) => Future.successful(Left(error))
            case None        =>
              val values = fetched.collect { case Right(value) => value }
              val present = values.collect { case DirectFetch(demand, Some(bytes)) =>
                demand -> bytes
              }
              AsyncBatch
                .traverseBounded(present, limits.maxConcurrentRequests): (demand, encoded) =>
                  ChunkGeometry.storedShape(descriptor.grid, demand.coordinate) match
                    case Left(error)       => Future.successful(Left(error))
                    case Right(chunkShape) =>
                      runtime
                        .decode(
                          encoded,
                          codecs,
                          descriptor.dataType,
                          chunkShape,
                          limits.decode
                        )
                        .map(_.map(block => DecodedCopy(demand.copy, block, chunkShape)))
                .map: decoded =>
                  firstError(decoded) match
                    case Some(error) => Left(error)
                    case None        =>
                      val copies = decoded.collect { case Right(value) => value }
                      applyCopies(builder, outputShape, copies) match
                        case Left(error) => Left(error)
                        case Right(_)    =>
                          Right(
                            ReadResult(
                              builder.result(),
                              outputShape,
                              ExecutionReceipt(
                                plan.demands.length,
                                0,
                                0,
                                present.map(_._2.byteCount.toLong).sum,
                                0L,
                                present.map(_._2.byteCount.toLong).sum,
                                plan.demands.length,
                                0,
                                plan.stats.requestedElements,
                                descriptor.dataType.byteWidth
                              )
                            )
                          )

  private final case class ShardLength(
      shard: ShardDemand,
      length: Option[Long]
  )

  private final case class IndexFetch(
      read: IndexRangeRead,
      bytes: Option[OwnedBytes]
  )

  private final case class DataFetch(
      shard: ShardDataRead,
      read: CoalescedRange[ShardChunkRead],
      bytes: Option[OwnedBytes]
  )

  private def readSharded(
      selection: ArraySelection,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      limits: ReadLimits
  ): Future[Either[ZarrError, ReadResult]] =
    val planned = for
      inner <- ChunkPlanner.plan(sharded.globalInnerGrid, selection, limits.planning)
      grouped <- ShardPlanner.group(sharded, inner)
    yield inner -> grouped
    planned match
      case Left(error) => Future.successful(Left(error))
      case Right((_, grouped)) if grouped.touchedShards > limits.maxObjects =>
        Future.successful(
          Left(
            ZarrError.ResourceLimit(
              "read shards",
              limits.maxObjects,
              grouped.touchedShards
            )
          )
        )
      case Right((innerPlan, grouped)) =>
        fetchLengths(grouped, location, limits).flatMap:
          case Left(error)    => Future.successful(Left(error))
          case Right(lengths) =>
            val missing = mutable.HashSet.empty[Vector[Long]]
            val objectLengths = mutable.Map.empty[Vector[Long], Long]
            lengths.foreach: found =>
              found.length match
                case None         => missing += found.shard.coordinate.toVector
                case Some(length) => objectLengths.update(found.shard.coordinate.toVector, length)
            val present =
              grouped.shards.filterNot(shard => missing.contains(shard.coordinate.toVector))
            val presentPlan =
              ShardReadPlan(present, present.map(_.innerChunks.length).sum, present.length)
            ShardIndexReadPlan(
              presentPlan,
              sharded.innerChunksPerShard,
              location,
              descriptor.chunkKeyEncoding,
              objectLengths.toMap,
              limits.shardIndex
            ) match
              case Left(error)      => Future.successful(Left(error))
              case Right(indexPlan) =>
                fetchIndexes(indexPlan, limits).flatMap:
                  case Left(error)         => Future.successful(Left(error))
                  case Right(indexFetches) =>
                    val indexes = mutable.Map.empty[Vector[Long], ShardIndex]
                    var indexBytes = 0L
                    var decodeError: Option[ZarrError] = None
                    indexFetches.foreach: fetched =>
                      fetched.bytes match
                        case None => missing += fetched.read.shardCoordinate.toVector
                        case Some(bytes) if decodeError.isEmpty =>
                          indexBytes += bytes.byteCount.toLong
                          ShardIndexCodec.decode(
                            bytes,
                            sharded.innerChunksPerShard,
                            limits.shardIndex
                          ) match
                            case Left(error)  => decodeError = Some(error)
                            case Right(index) =>
                              indexes.update(fetched.read.shardCoordinate.toVector, index)
                        case Some(_) => ()
                    decodeError match
                      case Some(error) => Future.successful(Left(error))
                      case None        =>
                        fillIndex(sharded.innerChunksPerShard, limits.shardIndex) match
                          case Left(error) => Future.successful(Left(error))
                          case Right(fill) =>
                            grouped.shards.foreach: shard =>
                              if missing.contains(shard.coordinate.toVector) then
                                indexes.update(shard.coordinate.toVector, fill)
                            ShardDataPlan.resolve(
                              grouped,
                              indexes.toMap,
                              descriptor.chunkKeyEncoding
                            ) match
                              case Left(error) => Future.successful(Left(error))
                              case Right(dataPlan) if dataPlan.rangeReads > limits.maxRanges =>
                                Future.successful(
                                  Left(
                                    ZarrError.ResourceLimit(
                                      "read ranges",
                                      limits.maxRanges,
                                      dataPlan.rangeReads
                                    )
                                  )
                                )
                              case Right(dataPlan) =>
                                fetchAndDecodeData(
                                  dataPlan,
                                  outputShape,
                                  builder,
                                  sharded,
                                  innerCodecs,
                                  limits
                                ).map:
                                  case Left(error)                      => Left(error)
                                  case Right((dataRequests, dataBytes)) =>
                                    Right(
                                      ReadResult(
                                        builder.result(),
                                        outputShape,
                                        ExecutionReceipt(
                                          (if location == IndexLocation.End then lengths.length
                                           else 0) +
                                            indexFetches.length + dataRequests,
                                          indexFetches.length + dataRequests,
                                          if location == IndexLocation.End then lengths.length
                                          else 0,
                                          indexBytes + dataBytes,
                                          indexBytes,
                                          dataBytes,
                                          grouped.touchedInnerChunks,
                                          grouped.touchedShards,
                                          innerPlan.stats.requestedElements,
                                          descriptor.dataType.byteWidth
                                        )
                                      )
                                    )

  private def fetchLengths(
      grouped: ShardReadPlan,
      location: IndexLocation,
      limits: ReadLimits
  ): Future[Either[ZarrError, Vector[ShardLength]]] =
    if location == IndexLocation.Start then
      Future.successful(Right(grouped.shards.map(shard => ShardLength(shard, Some(0L)))))
    else
      val fetched = AsyncBatch.traverseBounded(grouped.shards, limits.maxConcurrentRequests):
        shard =>
          chunkStoreKey(shard.coordinate) match
            case Left(error) => Future.successful(Left(error))
            case Right(key)  =>
              store
                .length(key)
                .map:
                  case Left(StoreError.NotFound(_)) => Right(ShardLength(shard, None))
                  case Left(error)                  => Left(ZarrError.StoreFailure(error))
                  case Right(length)                => Right(ShardLength(shard, Some(length)))
      fetched.map: results =>
        firstError(results).toLeft(results.collect { case Right(value) => value })

  private def fetchIndexes(
      plan: ShardIndexReadPlan,
      limits: ReadLimits
  ): Future[Either[ZarrError, Vector[IndexFetch]]] =
    val fetched = AsyncBatch.traverseBounded(plan.reads, limits.maxConcurrentRequests): read =>
      path.key(read.key.value) match
        case Left(error) => Future.successful(Left(error))
        case Right(key)  =>
          store
            .read(key, read.range)
            .map:
              case Left(StoreError.NotFound(_)) => Right(IndexFetch(read, None))
              case Left(error)                  => Left(ZarrError.StoreFailure(error))
              case Right(bytes)                 => Right(IndexFetch(read, Some(bytes)))
    fetched.map: results =>
      firstError(results).toLeft(results.collect { case Right(value) => value })

  private def fetchAndDecodeData(
      plan: ShardDataPlan,
      outputShape: Shape,
      builder: PrimitiveBlockBuilder,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      limits: ReadLimits
  ): Future[Either[ZarrError, (Int, Long)]] =
    val tasks = Vector.newBuilder[(ShardDataRead, CoalescedRange[ShardChunkRead])]
    var shardIndex = 0
    while shardIndex < plan.shards.length do
      val shard = plan.shards(shardIndex)
      val ranges = shard.chunks.collect:
        case chunk @ ShardChunkRead(_, ShardChunkSource.Range(range), _) => range -> chunk
      RangeCoalescer.coalesce(ranges, limits.coalescing) match
        case Left(error)  => return Future.successful(Left(error))
        case Right(reads) => reads.foreach(read => tasks += shard -> read)
      shardIndex += 1
    val taskValues = tasks.result()
    AsyncBatch
      .traverseBounded(taskValues, limits.maxConcurrentRequests): (shard, read) =>
        path.key(shard.key.value) match
          case Left(error) => Future.successful(Left(error))
          case Right(key)  =>
            store
              .read(key, read.range)
              .map:
                case Left(StoreError.NotFound(_)) => Right(DataFetch(shard, read, None))
                case Left(error)                  => Left(ZarrError.StoreFailure(error))
                case Right(bytes)                 => Right(DataFetch(shard, read, Some(bytes)))
      .flatMap: fetched =>
        firstError(fetched) match
          case Some(error) => Future.successful(Left(error))
          case None        =>
            val values = fetched.collect { case Right(value) => value }
            val members = Vector.newBuilder[(ShardDataRead, ShardChunkRead, OwnedBytes)]
            var bytesRead = 0L
            values.foreach: fetched =>
              fetched.bytes.foreach: bytes =>
                bytesRead += bytes.byteCount.toLong
                fetched.read.members.foreach: member =>
                  members += ((
                    fetched.shard,
                    member.value,
                    bytes.slice(member.relativeOffset, member.relativeOffset + member.length)
                  ))
            val memberValues = members.result()
            AsyncBatch
              .traverseBounded(memberValues, limits.maxConcurrentRequests):
                (shard, chunk, encoded) =>
                  val global =
                    globalInnerCoordinate(shard.coordinate, chunk.localCoordinate, sharded)
                  ChunkGeometry.storedShape(sharded.globalInnerGrid, global) match
                    case Left(error)       => Future.successful(Left(error))
                    case Right(chunkShape) =>
                      runtime
                        .decode(
                          encoded,
                          innerCodecs,
                          descriptor.dataType,
                          chunkShape,
                          limits.decode
                        )
                        .map(_.map(block => DecodedCopy(chunk.copy, block, chunkShape)))
              .map: decoded =>
                firstError(decoded) match
                  case Some(error) => Left(error)
                  case None        =>
                    applyCopies(
                      builder,
                      outputShape,
                      decoded.collect { case Right(value) => value }
                    ).map(_ => taskValues.length -> bytesRead)

  private def applyCopies(
      builder: PrimitiveBlockBuilder,
      outputShape: Shape,
      copies: Vector[DecodedCopy]
  ): Either[ZarrError, Unit] =
    var index = 0
    while index < copies.length do
      val copy = copies(index)
      PrimitiveBlockBuilder.applyCopy(
        builder,
        copy.block,
        copy.chunkShape,
        outputShape,
        copy.copy
      ) match
        case Left(error) => return Left(error)
        case Right(_)    => ()
      index += 1
    Right(())

  private def fillIndex(
      shape: Shape,
      limits: ShardIndexLimits
  ): Either[ZarrError, ShardIndex] = shape.elementCount.flatMap: count =>
    if count > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("fill shard index", Int.MaxValue, count))
    else ShardIndex(shape, Vector.fill(count.toInt)(ShardIndexEntry.Fill), limits)

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

  private def firstError[A](values: Vector[Either[ZarrError, A]]): Option[ZarrError] =
    values.collectFirst { case Left(error) => error }

enum AsyncOpenedNode:
  case Group(value: AsyncOpenedGroup)
  case Array(value: AsyncOpenedArray)

final class AsyncOpenedGroup private[zarr4s] (
    store: AsyncObjectReader,
    val path: ZarrPath,
    val metadata: GroupMetadata,
    val format: ZarrFormat,
    index: Option[HierarchyIndex],
    capabilities: ZarrCapabilities,
    limits: OpenLimits,
    runtime: AsyncCodecRuntime,
    consolidation: ConsolidationMode
)(using ExecutionContext):
  def children: Either[ZarrError, Vector[HierarchyEntry]] = index match
    case Some(found) => Right(found.children(path))
    case None        =>
      Left(
        ZarrError.UnsupportedRead(
          "hierarchy discovery requires consolidated metadata or a listing capability"
        )
      )

  def open(relativePath: String): Future[Either[ZarrError, AsyncOpenedNode]] =
    path.resolve(relativePath) match
      case Left(error)     => Future.successful(Left(error))
      case Right(resolved) =>
        index.flatMap(_.document(resolved)) match
          case Some(document) =>
            Future.successful(
              AsyncZarr.openDocument(
                store,
                resolved,
                document,
                index,
                capabilities,
                limits,
                runtime,
                consolidation
              )
            )
          case None if consolidation == ConsolidationMode.Require =>
            Future.successful(
              Left(
                ZarrError.InvalidMetadata(
                  "$.consolidated_metadata",
                  s"required metadata does not contain '${resolved.value}'"
                )
              )
            )
          case None =>
            AsyncZarr.openNode(
              store,
              resolved,
              capabilities,
              limits,
              runtime,
              ConsolidationMode.Ignore
            )

  def openArray(relativePath: String): Future[Either[ZarrError, AsyncOpenedArray]] =
    open(relativePath).map(_.flatMap:
      case AsyncOpenedNode.Array(found) => Right(found)
      case AsyncOpenedNode.Group(_)     => Left(ZarrError.UnsupportedNodeType("group")))

  def openGroup(relativePath: String): Future[Either[ZarrError, AsyncOpenedGroup]] =
    open(relativePath).map(_.flatMap:
      case AsyncOpenedNode.Group(found) => Right(found)
      case AsyncOpenedNode.Array(_)     => Left(ZarrError.UnsupportedNodeType("array")))

object AsyncZarr:
  def openArray(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedArray]] =
    openNode(store, path, capabilities, limits, runtime, consolidation).map(_.flatMap:
      case AsyncOpenedNode.Array(found) => Right(found)
      case AsyncOpenedNode.Group(_)     => Left(ZarrError.UnsupportedNodeType("group")))

  def openGroup(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedGroup]] =
    openNode(store, path, capabilities, limits, runtime, consolidation).map(_.flatMap:
      case AsyncOpenedNode.Group(found) => Right(found)
      case AsyncOpenedNode.Array(_)     => Left(ZarrError.UnsupportedNodeType("array")))

  def openNode(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedNode]] =
    readOptional(store, path, "zarr.json", limits.maxMetadataBytes).flatMap:
      case Left(error)       => Future.successful(Left(error))
      case Right(Some(json)) =>
        ZarrMetadata.parse(json) match
          case Left(error)                          => Future.successful(Left(error))
          case Right(ZarrNodeMetadata.Group(group)) =>
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
            Future.successful(indexed.map: found =>
              AsyncOpenedNode.Group(
                new AsyncOpenedGroup(
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
              ))
          case Right(ZarrNodeMetadata.Array(array)) =>
            Future.successful(
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
            )
      case Right(None) => openV2(store, path, capabilities, limits, runtime, consolidation)

  private[zarr4s] def openDocument(
      store: AsyncObjectReader,
      path: ZarrPath,
      document: HierarchyDocument,
      index: Option[HierarchyIndex],
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: AsyncCodecRuntime,
      consolidation: ConsolidationMode
  )(using ExecutionContext): Either[ZarrError, AsyncOpenedNode] = document.kind match
    case NodeKind.Group =>
      document.groupMetadata.map: group =>
        AsyncOpenedNode.Group(
          new AsyncOpenedGroup(
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
                  AsyncOpenedNode.Array(
                    new AsyncOpenedArray(
                      store,
                      path,
                      descriptor,
                      document.format,
                      runtime
                    )
                  )

  private def openV2(
      store: AsyncObjectReader,
      path: ZarrPath,
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: AsyncCodecRuntime,
      consolidation: ConsolidationMode
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedNode]] =
    val indexed = consolidation match
      case ConsolidationMode.Ignore => Future.successful(Right(None))
      case _                        =>
        readOptional(store, ZarrPath.root, ".zmetadata", limits.maxMetadataBytes).map:
          case Left(error)       => Left(error)
          case Right(Some(json)) =>
            HierarchyIndex.v2(ZarrPath.root, json, limits.hierarchy).map(Some.apply)
          case Right(None) if consolidation == ConsolidationMode.Require =>
            Left(
              ZarrError.InvalidMetadata(
                "$.zmetadata",
                "required consolidated metadata is absent"
              )
            )
          case Right(None) => Right(None)
    indexed.flatMap:
      case Left(error)  => Future.successful(Left(error))
      case Right(found) =>
        found.flatMap(_.document(path)) match
          case Some(document) =>
            Future.successful(
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
            )
          case None if consolidation == ConsolidationMode.Require =>
            Future.successful(
              Left(
                ZarrError.InvalidMetadata(
                  "$.zmetadata.metadata",
                  s"required metadata does not contain '${path.value}'"
                )
              )
            )
          case None => openV2Individual(store, path, capabilities, limits, runtime)

  private def openV2Individual(
      store: AsyncObjectReader,
      path: ZarrPath,
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: AsyncCodecRuntime
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedNode]] =
    readOptional(store, path, ".zarray", limits.maxMetadataBytes).flatMap:
      case Left(error)            => Future.successful(Left(error))
      case Right(Some(arrayJson)) =>
        readOptional(store, path, ".zattrs", limits.maxMetadataBytes).map:
          case Left(error)       => Left(error)
          case Right(attributes) =>
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
      case Right(None) =>
        readOptional(store, path, ".zgroup", limits.maxMetadataBytes).flatMap:
          case Left(error)            => Future.successful(Left(error))
          case Right(Some(groupJson)) =>
            readOptional(store, path, ".zattrs", limits.maxMetadataBytes).map:
              case Left(error)       => Left(error)
              case Right(attributes) =>
                V2Metadata
                  .parseGroup(groupJson, attributes)
                  .map: group =>
                    AsyncOpenedNode.Group(
                      new AsyncOpenedGroup(
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
          case Right(None) =>
            Future.successful(
              path
                .key(".zarray")
                .flatMap: key =>
                  Left(ZarrError.StoreFailure(StoreError.NotFound(key)))
            )

  private def readOptional(
      store: AsyncObjectReader,
      path: ZarrPath,
      child: String,
      limit: ByteCount
  )(using ExecutionContext): Future[Either[ZarrError, Option[String]]] = path.key(child) match
    case Left(error) => Future.successful(Left(error))
    case Right(key)  =>
      store
        .readAll(key, limit)
        .map:
          case Left(StoreError.NotFound(_)) => Right(None)
          case Left(error)                  => Left(ZarrError.StoreFailure(error))
          case Right(bytes)                 => Right(Some(new String(bytes.values, "UTF-8")))
