package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Scala.js compatibility names for the portable asynchronous reader. */
type BrowserOpenedArray = AsyncOpenedArray
type BrowserOpenedGroup = AsyncOpenedGroup
type BrowserOpenedNode = AsyncOpenedNode
type BrowserTypedOpenedArray[D <: DType] = AsyncTypedOpenedArray[D]

object BrowserOpenedNode:
  export AsyncOpenedNode.*

/** Browser-oriented facade that adds the browser gzip and zlib executors by default.
  *
  * The reader implementation itself is portable and lives in [[AsyncZarr]].
  */
object BrowserZarr:
  def createArray[D <: DType](
      store: AsyncObjectWriter,
      spec: ArraySpec[D],
      data: DenseArray[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using ExecutionContext): Future[Either[ZarrError, TypedWriteResult[D]]] =
    AsyncZarr.createArray(
      store,
      spec,
      data,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      capabilities
    )

  def createArrayFromProvider[D <: DType](
      store: AsyncObjectWriter,
      spec: ArraySpec[D],
      provider: TypedChunkProvider[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using ExecutionContext): Future[Either[ZarrError, TypedWriteResult[D]]] =
    AsyncZarr.createArrayFromProvider(
      store,
      spec,
      provider,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      capabilities
    )

  def createFillArray[D <: DType](
      store: AsyncObjectWriter,
      spec: ArraySpec[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using ExecutionContext): Future[Either[ZarrError, TypedWriteResult[D]]] =
    AsyncZarr.createFillArray(
      store,
      spec,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      capabilities
    )

  def createAndOpenArray[D <: DType](
      store: AsyncObjectWriter & AsyncObjectReader,
      spec: ArraySpec[D],
      data: DenseArray[D],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      openLimits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using ExecutionContext): Future[Either[ZarrError, AsyncTypedCreateAndOpen[D]]] =
    AsyncZarr.createAndOpenArray(
      store,
      spec,
      data,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      openLimits,
      capabilities
    )

  def openTypedArray[D <: DType](
      store: AsyncObjectReader,
      dtype: D,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer,
      lister: Option[AsyncObjectLister] = None
  )(using ExecutionContext): Future[Either[ZarrError, BrowserTypedOpenedArray[D]]] =
    AsyncZarr.openTypedArray(
      store,
      dtype,
      path,
      capabilities,
      limits,
      runtime,
      consolidation,
      lister
    )

  def openArray(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer,
      lister: Option[AsyncObjectLister] = None
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedArray]] =
    AsyncZarr.openArray(store, path, capabilities, limits, runtime, consolidation, lister)

  def openGroup(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer,
      lister: Option[AsyncObjectLister] = None
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedGroup]] =
    AsyncZarr.openGroup(store, path, capabilities, limits, runtime, consolidation, lister)

  def openNode(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer,
      lister: Option[AsyncObjectLister] = None
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedNode]] =
    AsyncZarr.openNode(store, path, capabilities, limits, runtime, consolidation, lister)
