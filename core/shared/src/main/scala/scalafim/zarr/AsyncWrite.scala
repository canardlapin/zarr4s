package scalafim.zarr

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

/** Asynchronous create-only interpreter with one in-flight provider, codec, or
  * object-store effect at a time. This is a portable backpressure contract,
  * not an implicit concurrency policy.
  */
object AsyncZarrWriter:
  def create(
      store: AsyncObjectWriter,
      descriptor: ArrayDescriptor,
      provider: AsyncChunkProvider,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core
  )(using ExecutionContext): Future[WriteOutcome] =
    val metrics = new WriteMetrics
    val prepared = try for
      _ <- OpenValidation.codecPrograms(descriptor, runtime.validate)
      metadata <- WriteInternals.arrayMetadata(descriptor, path, limits)
    yield metadata
    catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))
    prepared match
      case Left(error) => Future.successful(WriteOutcome.Incomplete(metrics.snapshot, error))
      case Right(metadata) =>
        val writing = writeArray(
          store,
          descriptor,
          provider,
          path,
          metadata._2.byteCount,
          limits,
          runtime,
          metrics
        ).flatMap:
          case Left(error) => Future.successful(WriteOutcome.Incomplete(metrics.snapshot, error))
          case Right(_) => finish(store, metadata, metrics).map:
            case Left(error) => WriteOutcome.Incomplete(metrics.snapshot, error)
            case Right(receipt) => WriteOutcome.Complete(receipt)
        writing.recover:
          case NonFatal(error) => WriteOutcome.Incomplete(
            metrics.snapshot,
            ZarrError.WriteFailure(error.getMessage)
          )

  def createGroup(
      store: AsyncObjectWriter,
      metadata: GroupMetadata,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits()
  )(using ExecutionContext): Future[WriteOutcome] =
    val metrics = new WriteMetrics
    val rendered = try WriteInternals.groupMetadata(metadata, path, limits)
    catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))
    rendered match
      case Left(error) => Future.successful(WriteOutcome.Incomplete(metrics.snapshot, error))
      case Right(found) =>
        val writing = finish(store, found, metrics).map:
          case Left(error) => WriteOutcome.Incomplete(metrics.snapshot, error)
          case Right(receipt) => WriteOutcome.Complete(receipt)
        writing.recover:
          case NonFatal(error) => WriteOutcome.Incomplete(
            metrics.snapshot,
            ZarrError.WriteFailure(error.getMessage)
          )

  private def writeArray(
      store: AsyncObjectWriter,
      descriptor: ArrayDescriptor,
      provider: AsyncChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: AsyncCodecRuntime,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, Unit]] = descriptor.layout match
    case PhysicalLayout.Direct(codecs) => writeDirect(
      store,
      descriptor,
      codecs,
      provider,
      path,
      metadataLength,
      limits,
      runtime,
      metrics
    )
    case PhysicalLayout.Sharded(sharded, innerCodecs, _, location, outerCodecs) =>
      if outerCodecs.nonEmpty then Future.successful(Left(ZarrError.UnsupportedWrite(
        "outer codecs after sharding_indexed are not supported"
      )))
      else writeSharded(
        store,
        descriptor,
        sharded,
        innerCodecs,
        location,
        provider,
        path,
        metadataLength,
        limits,
        runtime,
        metrics
      )

  private def writeDirect(
      store: AsyncObjectWriter,
      descriptor: ArrayDescriptor,
      codecs: CodecProgram,
      provider: AsyncChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: AsyncCodecRuntime,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, Unit]] =
    foreachCoordinate(descriptor.grid.gridShape): coordinate =>
      metrics.visit(limits) match
        case Left(error) => Future.successful(Left(error))
        case Right(_) => ChunkGeometry.storedShape(descriptor.grid, coordinate) match
          case Left(error) => Future.successful(Left(error))
          case Right(storedShape) => safe(provider.chunk(coordinate, storedShape)).flatMap:
            case Left(error) => Future.successful(Left(error))
            case Right(ChunkPayload.Fill) => Future.successful(metrics.omitFill())
            case Right(ChunkPayload.Values(block)) =>
              encodeChunk(block, storedShape, descriptor, codecs, limits, runtime, metrics).flatMap:
                case Left(error) => Future.successful(Left(error))
                case Right(encoded) => WriteInternals.resolve(
                  path,
                  descriptor.chunkKeyEncoding.encode(coordinate)
                ) match
                  case Left(error) => Future.successful(Left(error))
                  case Right(key) => createDataObject(
                    store,
                    key,
                    encoded,
                    metadataLength,
                    limits,
                    metrics
                  )

  private def writeSharded(
      store: AsyncObjectWriter,
      descriptor: ArrayDescriptor,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      provider: AsyncChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: AsyncCodecRuntime,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, Unit]] =
    foreachCoordinate(sharded.outerGrid.gridShape): shardCoordinate =>
      encodeShard(
        descriptor,
        sharded,
        shardCoordinate,
        innerCodecs,
        location,
        provider,
        limits,
        runtime,
        metrics
      ).flatMap:
        case Left(error) => Future.successful(Left(error))
        case Right(None) => Future.successful(Right(()))
        case Right(Some(encoded)) => WriteInternals.resolve(
          path,
          descriptor.chunkKeyEncoding.encode(shardCoordinate)
        ) match
          case Left(error) => Future.successful(Left(error))
          case Right(key) => createDataObject(
            store,
            key,
            encoded,
            metadataLength,
            limits,
            metrics
          )

  private def encodeShard(
      descriptor: ArrayDescriptor,
      sharded: ShardedGrid,
      shardCoordinate: ChunkCoordinate,
      innerCodecs: CodecProgram,
      location: IndexLocation,
      provider: AsyncChunkProvider,
      limits: WriterLimits,
      runtime: AsyncCodecRuntime,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, Option[OwnedBytes]]] =
    val chunks = Vector.newBuilder[Option[OwnedBytes]]
    val visited = foreachCoordinate(sharded.innerChunksPerShard): local =>
      val global = WriteInternals.globalInnerCoordinate(shardCoordinate, local, sharded)
      if !WriteInternals.insideGrid(global, sharded.globalInnerGrid.gridShape) then
        Future.successful(metrics.omitPadding().map: _ =>
          chunks += None
          ()
        )
      else metrics.visit(limits) match
        case Left(error) => Future.successful(Left(error))
        case Right(_) => ChunkGeometry.storedShape(sharded.globalInnerGrid, global) match
          case Left(error) => Future.successful(Left(error))
          case Right(storedShape) => safe(provider.chunk(global, storedShape)).flatMap:
            case Left(error) => Future.successful(Left(error))
            case Right(ChunkPayload.Fill) =>
              Future.successful(metrics.omitFill().map: _ =>
                chunks += None
                ()
              )
            case Right(ChunkPayload.Values(block)) =>
              val encoding = encodeChunk(
                block,
                storedShape,
                descriptor,
                innerCodecs,
                limits,
                runtime,
                metrics
              )
              encoding.map: result =>
                result.map: encoded =>
                  chunks += Some(encoded)
                  ()
    visited.map(result => result.flatMap(_ => WriteInternals.assembleShard(
        chunks.result(),
        sharded.innerChunksPerShard,
        location,
        limits
      )))

  private def encodeChunk(
      block: PrimitiveBlock,
      storedShape: Shape,
      descriptor: ArrayDescriptor,
      codecs: CodecProgram,
      limits: WriterLimits,
      runtime: AsyncCodecRuntime,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, OwnedBytes]] =
    storedShape.elementCount match
      case Left(error) => Future.successful(Left(error))
      case Right(expected) => PrimitiveBlockType.validate(
          block,
          descriptor.dataType,
          expected
        ) match
          case Left(error) => Future.successful(Left(error))
          case Right(_) =>
            val encoding = runtime.encode(
              block,
              descriptor.dataType,
              storedShape,
              codecs,
              limits.maxEncodedChunkBytes
            )
            encoding.map: result =>
              result.flatMap: encoded =>
                metrics.encodedChunk().map(_ => encoded)

  private def createDataObject(
      store: AsyncObjectWriter,
      key: StoreKey,
      bytes: OwnedBytes,
      metadataLength: ByteCount,
      limits: WriterLimits,
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, Unit]] =
    metrics.permitDataObject(bytes.byteCount, metadataLength, limits) match
      case Left(error) => Future.successful(Left(error))
      case Right(_) => safe(store.create(key, bytes)).map:
        case Left(error) => Left(ZarrError.StoreFailure(error))
        case Right(_) => metrics.record(WriteInternals.written(key, bytes))

  private def finish(
      store: AsyncObjectWriter,
      metadata: (StoreKey, OwnedBytes),
      metrics: WriteMetrics
  )(using ExecutionContext): Future[Either[ZarrError, WriteReceipt]] =
    val (key, bytes) = metadata
    safe(store.create(key, bytes)).map:
      case Left(error) => Left(ZarrError.StoreFailure(error))
      case Right(_) => Right(new WriteReceipt(
        metrics.snapshot,
        WriteInternals.written(key, bytes)
      ))

  private def foreachCoordinate(
      shape: Shape
  )(
      operation: ChunkCoordinate => Future[Either[ZarrError, Unit]]
  )(using ExecutionContext): Future[Either[ZarrError, Unit]] =
    if shape.values.exists(_ == 0L) then Future.successful(Right(()))
    else if shape.rank.toInt == 0 then safe(operation(ChunkCoordinate.unsafe(Array.emptyLongArray)))
    else
      val current = new Array[Long](shape.rank.toInt)
      def loop(): Future[Either[ZarrError, Unit]] =
        safe(operation(ChunkCoordinate.unsafe(current))).flatMap:
          case Left(error) => Future.successful(Left(error))
          case Right(_) =>
            WriteInternals.advance(current, shape)
            if current.forall(_ == 0L) then Future.successful(Right(()))
            else loop()
      loop()

  private def safe[A](future: => Future[A])(using ExecutionContext): Future[A] =
    try future
    catch case NonFatal(error) => Future.failed(error)
