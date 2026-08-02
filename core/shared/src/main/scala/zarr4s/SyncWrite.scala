package zarr4s

import scala.util.control.NonFatal

/** Store-independent, create-only Zarr writer.
  *
  * Data objects are created in deterministic grid order and the primary metadata object is created
  * last. An incomplete outcome is intentionally observable because a generic object store cannot
  * promise namespace rollback.
  */
object SyncZarrWriter:
  def create(
      store: ObjectWriter,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      format: ZarrFormat = ZarrFormat.V3
  ): WriteOutcome =
    val metrics = new WriteMetrics
    val writeDescriptor = WriteInternals.descriptorForWrite(descriptor, format)
    val result =
      try
        for
          _ <- OpenValidation.codecPrograms(descriptor, runtime.validate)
          metadata <- WriteInternals.arrayMetadata(descriptor, path, limits, format)
          _ <- publishPrelude(store, metadata, limits, metrics)
          _ <- writeArray(
            store,
            writeDescriptor,
            provider,
            path,
            metadata.primaryBytes.byteCount,
            limits,
            runtime,
            metrics
          )
          receipt <- finish(store, metadata, metrics)
        yield receipt
      catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))
    outcome(result, metrics)

  def createGroup(
      store: ObjectWriter,
      metadata: GroupMetadata,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      format: ZarrFormat = ZarrFormat.V3
  ): WriteOutcome =
    val metrics = new WriteMetrics
    val result =
      try
        for
          rendered <- WriteInternals.groupMetadata(metadata, path, limits, format)
          _ <- publishPrelude(store, rendered, limits, metrics)
          receipt <- finish(store, rendered, metrics)
        yield receipt
      catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))
    outcome(result, metrics)

  private def writeArray(
      store: ObjectWriter,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: SyncCodecRuntime,
      metrics: WriteMetrics
  ): Either[ZarrError, Unit] = descriptor.layout match
    case PhysicalLayout.Direct(codecs) =>
      writeDirect(
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
    case PhysicalLayout.Sharded(sharded, innerCodecs, indexCodecs, location, outerCodecs) =>
      writeSharded(
        store,
        descriptor,
        sharded,
        innerCodecs,
        indexCodecs,
        location,
        outerCodecs,
        provider,
        path,
        metadataLength,
        limits,
        runtime,
        metrics
      )

  private def writeDirect(
      store: ObjectWriter,
      descriptor: ArrayDescriptor,
      codecs: CodecProgram,
      provider: ChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: SyncCodecRuntime,
      metrics: WriteMetrics
  ): Either[ZarrError, Unit] =
    WriteInternals.foreachCoordinate(descriptor.grid.gridShape): coordinate =>
      metrics
        .visit(limits)
        .flatMap: _ =>
          ChunkGeometry
            .storedShape(descriptor.grid, coordinate)
            .flatMap: storedShape =>
              provider
                .chunk(coordinate, storedShape)
                .flatMap:
                  case ChunkPayload.Fill          => metrics.omitFill()
                  case ChunkPayload.Values(block) =>
                    for
                      expected <- storedShape.elementCount
                      _ <- PrimitiveBlockType.validate(block, descriptor.dataType, expected)
                      encoded <- runtime.encode(
                        block,
                        descriptor.dataType,
                        storedShape,
                        codecs,
                        limits.maxEncodedChunkBytes
                      )
                      _ <- metrics.encodedChunk()
                      relative = descriptor.chunkKeyEncoding.encode(coordinate)
                      key <- WriteInternals.resolve(path, relative)
                      _ <- createDataObject(store, key, encoded, metadataLength, limits, metrics)
                    yield ()

  private def writeSharded(
      store: ObjectWriter,
      descriptor: ArrayDescriptor,
      sharded: ShardedGrid,
      innerCodecs: CodecProgram,
      indexCodecs: ShardIndexProgram,
      location: IndexLocation,
      outerCodecs: CodecProgram,
      provider: ChunkProvider,
      path: ZarrPath,
      metadataLength: ByteCount,
      limits: WriterLimits,
      runtime: SyncCodecRuntime,
      metrics: WriteMetrics
  ): Either[ZarrError, Unit] =
    WriteInternals.foreachCoordinate(sharded.outerGrid.gridShape): shardCoordinate =>
      encodeShard(
        descriptor,
        sharded,
        shardCoordinate,
        innerCodecs,
        indexCodecs,
        location,
        outerCodecs,
        provider,
        limits,
        runtime,
        metrics
      ).flatMap:
        case None          => Right(())
        case Some(encoded) =>
          val relative = descriptor.chunkKeyEncoding.encode(shardCoordinate)
          for
            key <- WriteInternals.resolve(path, relative)
            _ <- createDataObject(store, key, encoded, metadataLength, limits, metrics)
          yield ()

  private def encodeShard(
      descriptor: ArrayDescriptor,
      sharded: ShardedGrid,
      shardCoordinate: ChunkCoordinate,
      innerCodecs: CodecProgram,
      indexCodecs: ShardIndexProgram,
      location: IndexLocation,
      outerCodecs: CodecProgram,
      provider: ChunkProvider,
      limits: WriterLimits,
      runtime: SyncCodecRuntime,
      metrics: WriteMetrics
  ): Either[ZarrError, Option[OwnedBytes]] =
    val encodedChunks = Vector.newBuilder[Option[OwnedBytes]]
    val visited = WriteInternals.foreachCoordinate(sharded.innerChunksPerShard): local =>
      val global = WriteInternals.globalInnerCoordinate(shardCoordinate, local, sharded)
      if !WriteInternals.insideGrid(global, sharded.globalInnerGrid.gridShape) then
        metrics
          .omitPadding()
          .map: _ =>
            encodedChunks += None
            ()
      else
        metrics
          .visit(limits)
          .flatMap: _ =>
            ChunkGeometry
              .storedShape(sharded.globalInnerGrid, global)
              .flatMap: storedShape =>
                provider
                  .chunk(global, storedShape)
                  .flatMap:
                    case ChunkPayload.Fill =>
                      metrics
                        .omitFill()
                        .map: _ =>
                          encodedChunks += None
                          ()
                    case ChunkPayload.Values(block) =>
                      for
                        expected <- storedShape.elementCount
                        _ <- PrimitiveBlockType.validate(block, descriptor.dataType, expected)
                        encoded <- runtime.encode(
                          block,
                          descriptor.dataType,
                          storedShape,
                          innerCodecs,
                          limits.maxEncodedChunkBytes
                        )
                        _ <- metrics.encodedChunk()
                      yield
                        encodedChunks += Some(encoded)
                        ()
    visited.flatMap(_ =>
      WriteInternals.assembleShard(
        encodedChunks.result(),
        sharded.innerChunksPerShard,
        indexCodecs,
        location,
        outerCodecs,
        limits,
        runtime
      )
    )

  private def createDataObject(
      store: ObjectWriter,
      key: StoreKey,
      bytes: OwnedBytes,
      metadataLength: ByteCount,
      limits: WriterLimits,
      metrics: WriteMetrics
  ): Either[ZarrError, Unit] = for
    _ <- metrics.permitDataObject(bytes.byteCount, metadataLength, limits)
    _ <- store.create(key, bytes).left.map(ZarrError.StoreFailure.apply)
    _ <- metrics.record(WriteInternals.written(key, bytes))
  yield ()

  private def publishPrelude(
      store: ObjectWriter,
      metadata: WriteInternals.MetadataObjects,
      limits: WriterLimits,
      metrics: WriteMetrics
  ): Either[ZarrError, Unit] = metadata.prelude match
    case None               => Right(())
    case Some((key, bytes)) =>
      for
        _ <- metrics.permitMetadataObject(bytes.byteCount, metadata.primaryBytes.byteCount, limits)
        _ <- store.create(key, bytes).left.map(ZarrError.StoreFailure.apply)
        _ <- metrics.recordMetadata(WriteInternals.written(key, bytes))
      yield ()

  private def finish(
      store: ObjectWriter,
      metadata: WriteInternals.MetadataObjects,
      metrics: WriteMetrics
  ): Either[ZarrError, WriteReceipt] =
    val (key, bytes) = metadata.primary
    store
      .create(key, bytes)
      .left
      .map(ZarrError.StoreFailure.apply)
      .map: _ =>
        new WriteReceipt(metrics.snapshot, WriteInternals.written(key, bytes))

  private def outcome(
      result: Either[ZarrError, WriteReceipt],
      metrics: WriteMetrics
  ): WriteOutcome = result match
    case Right(receipt) => WriteOutcome.Complete(receipt)
    case Left(error)    => WriteOutcome.Incomplete(metrics.snapshot, error)
