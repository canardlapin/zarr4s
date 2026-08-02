package zarr4s

import java.nio.file.Path

/** JVM filesystem facade for the typed create-only workflow.
  *
  * The blocking boundary is explicit in this JVM-only API. Publication delegates to the existing
  * staged [[JvmZarrWriter]] and therefore never exposes a partially published target as success.
  */
object JvmZarr:
  def createArray[D <: DType](
      target: Path,
      spec: ArraySpec[D],
      data: DenseArray[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, TypedWriteResult[D]] =
    for
      descriptor <- TypedWriteSupport.descriptor(spec, sharding, codecs, chunkKey, capabilities)
      provider <- TypedWriteSupport.denseProvider(descriptor, spec, data)
    yield TypedWriteResult(
      spec,
      descriptor,
      JvmZarrWriter.createOutcome(target, descriptor, provider, limits, runtime, spec.format)
    )

  def createArrayFromProvider[D <: DType](
      target: Path,
      spec: ArraySpec[D],
      provider: TypedChunkProvider[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, TypedWriteResult[D]] =
    for
      descriptor <- TypedWriteSupport.descriptor(spec, sharding, codecs, chunkKey, capabilities)
      checked <- TypedWriteSupport.typedProvider(spec, provider)
    yield TypedWriteResult(
      spec,
      descriptor,
      JvmZarrWriter.createOutcome(target, descriptor, checked, limits, runtime, spec.format)
    )

  def createFillArray[D <: DType](
      target: Path,
      spec: ArraySpec[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, TypedWriteResult[D]] =
    TypedWriteSupport
      .descriptor(spec, sharding, codecs, chunkKey, capabilities)
      .map: descriptor =>
        TypedWriteResult(
          spec,
          descriptor,
          JvmZarrWriter.createOutcome(
            target,
            descriptor,
            ChunkProvider.fill(descriptor),
            limits,
            runtime,
            spec.format
          )
        )

  def createAndOpenArray[D <: DType](
      target: Path,
      spec: ArraySpec[D],
      data: DenseArray[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      limits: WriterLimits = WriterLimits(),
      openLimits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, TypedCreateAndOpen[D]] =
    createArray(target, spec, data, sharding, codecs, chunkKey, limits, runtime, capabilities).map:
      result =>
        result.outcome match
          case WriteOutcome.Incomplete(_, error) =>
            TypedCreateAndOpen(result, Left(error))
          case WriteOutcome.Complete(_) =>
            JvmFileStore.open(target) match
              case Left(detail) =>
                TypedCreateAndOpen(result, Left(ZarrError.WriteFailure(detail)))
              case Right(store) =>
                TypedCreateAndOpen(
                  result,
                  SyncZarr.openTypedArray(
                    store,
                    spec.dtype,
                    capabilities = capabilities,
                    limits = openLimits,
                    runtime = runtime
                  )
                )
