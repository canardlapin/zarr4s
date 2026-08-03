package zarr4s

import java.nio.file.Path

/** JVM filesystem facade for typed creation and opening.
  *
  * The blocking boundary is explicit in this JVM-only API. Publication delegates to the existing
  * staged [[JvmZarrWriter]] and therefore never exposes a partially published target as success.
  */
object JvmZarr:
  /** Create a group with staged atomic directory publication. */
  def createGroup(
      target: Path,
      spec: GroupSpec = GroupSpec(),
      limits: WriterLimits = WriterLimits()
  ): GroupWriteResult =
    GroupWriteResult(
      spec,
      JvmZarrWriter.createGroupOutcome(
        target,
        GroupMetadata(spec.attributes, JsonObject.empty),
        limits,
        spec.format
      )
    )

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
            JvmFileStore.openChecked(target) match
              case Left(error) =>
                TypedCreateAndOpen(result, Left(error))
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

  /** Open any Zarr node rooted at an existing filesystem directory. */
  def openNode(
      root: Path,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedNode] =
    withStore(root): store =>
      SyncZarr.openNode(
        store,
        path,
        capabilities,
        limits,
        runtime,
        consolidation,
        Some(store)
      )

  /** Open an array rooted at an existing filesystem directory. */
  def openArray(
      root: Path,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedArray] =
    openNode(root, path, capabilities, limits, runtime, consolidation).flatMap:
      case OpenedNode.Array(found) => Right(found)
      case OpenedNode.Group(_)     => Left(ZarrError.UnsupportedNodeType("group"))

  /** Open an array and verify its dtype at the filesystem boundary. */
  def openTypedArray[D <: DType](
      root: Path,
      dtype: D,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, TypedOpenedArray[D]] =
    openArray(root, path, capabilities, limits, runtime, consolidation).flatMap(_.asTyped(dtype))

  /** Open a group rooted at an existing filesystem directory. */
  def openGroup(
      root: Path,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  ): Either[ZarrError, OpenedGroup] =
    openNode(root, path, capabilities, limits, runtime, consolidation).flatMap:
      case OpenedNode.Group(found) => Right(found)
      case OpenedNode.Array(_)     => Left(ZarrError.UnsupportedNodeType("array"))

  private def withStore[A](root: Path)(
      use: JvmFileStore => Either[ZarrError, A]
  ): Either[ZarrError, A] =
    JvmFileStore.openChecked(root).flatMap(use)
