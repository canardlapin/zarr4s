package example

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import zarr4s.*

final case class IdentityCodec(configuration: JsonObject) extends CompiledCodec:
  val name = IdentityCodec.name
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes

object IdentityCodec:
  val name = "example.identity"

object IdentityCapability extends CodecCapability:
  val name = IdentityCodec.name

  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec] =
    Right(IdentityCodec(extension.configuration))

object IdentitySyncExecutor extends SyncByteCodecExecutor:
  val name = IdentityCodec.name

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] =
    DecodedLength.validate(encoded, expectedDecoded, limits)

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = Right(decoded)

object IdentityAsyncExecutor extends AsyncByteCodecExecutor:
  val name = IdentityCodec.name

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(DecodedLength.validate(encoded, expectedDecoded, limits))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(Right(decoded))

object StandaloneConsumer:
  val capabilities = ZarrCapabilities(
    codecs = BuiltInCodecs.all :+ IdentityCapability
  )

  val syncRuntime: Either[ZarrError, SyncCodecRuntime] =
    SyncCodecRuntime("standalone JVM", Vector(IdentitySyncExecutor))

  val asyncRuntime: Either[ZarrError, AsyncCodecRuntime] =
    AsyncCodecRuntime("standalone Scala.js", Vector(IdentityAsyncExecutor))

  val descriptor: Either[ZarrError, ArrayDescriptor] =
    val metadata =
      """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"example.identity"}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""
    ZarrMetadata.parse(metadata).flatMap:
      case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array, capabilities)
      case ZarrNodeMetadata.Group(_) => Left(ZarrError.UnsupportedNodeType("group"))

  private val values = PrimitiveBlock.Int16(
    OwnedShorts.copyOf(Array[Short](1, 2, 3, 4, 5, 6))
  )

  val chunkProvider: ChunkProvider = new ChunkProvider:
    def chunk(
        coordinate: ChunkCoordinate,
        storedShape: Shape
    ): Either[ZarrError, ChunkPayload] = Right(ChunkPayload.Values(values))

  val asyncChunkProvider: AsyncChunkProvider = AsyncChunkProvider.fromSync(chunkProvider)

  def createSync(
      store: ObjectWriter,
      path: ZarrPath
  ): Either[ZarrError, WriteOutcome] = for
    found <- descriptor
    runtime <- syncRuntime
  yield SyncZarrWriter.create(
    store,
    found,
    chunkProvider,
    path,
    runtime = runtime
  )

  def createAsync(
      store: AsyncObjectWriter,
      path: ZarrPath
  )(using ExecutionContext): Future[Either[ZarrError, WriteOutcome]] =
    descriptor.flatMap(found => asyncRuntime.map(found -> _)) match
      case Left(error) => Future.successful(Left(error))
      case Right((found, runtime)) => AsyncZarrWriter.create(
        store,
        found,
        asyncChunkProvider,
        path,
        runtime = runtime
      ).map(Right.apply)

  def cachedSyncReader(
      store: ObjectReader,
      revision: String
  ): Either[ZarrError, CachingObjectReader] =
    CacheNamespace.from(revision).map: namespace =>
      CachingObjectReader(store, ObjectReadCache(namespace))

  def cachedAsyncReader(
      store: AsyncObjectReader,
      revision: String
  )(using ExecutionContext): Either[ZarrError, CachingAsyncObjectReader] =
    CacheNamespace.from(revision).map: namespace =>
      CachingAsyncObjectReader(store, ObjectReadCache(namespace))

  def openAsync(
      store: AsyncObjectReader,
      path: ZarrPath
  )(using ExecutionContext): Future[Either[ZarrError, AsyncOpenedArray]] =
    asyncRuntime match
      case Left(error) => Future.successful(Left(error))
      case Right(runtime) => AsyncZarr.openArray(
        store,
        path,
        capabilities,
        runtime = runtime
      )
