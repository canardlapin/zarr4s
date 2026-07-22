package scalafim.zarr.external

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scalafim.zarr.*

/** Test-only codec provider: it deliberately lives outside `scalafim.zarr`. */
final case class ExternalXorCodec(
    mask: Int,
    configuration: JsonObject
) extends CompiledCodec:
  val name = ExternalXorCodec.name
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes

object ExternalXorCodec:
  val name = "org.scalafim.test.xor"

object ExternalXorCapability extends CodecCapability:
  val name = ExternalXorCodec.name

  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec] = extension.configuration.get("mask") match
    case Some(JsonValue.Num(number)) => number.toLongExact.flatMap: mask =>
      if mask < 1L || mask > 255L then Left(s"xor mask must be in [1, 255], found $mask")
      else Right(ExternalXorCodec(mask.toInt, extension.configuration))
    case Some(_) => Left("xor mask must be an integer")
    case None => Left("missing required field 'mask'")

object ExternalXorSyncExecutor extends SyncByteCodecExecutor:
  val name = ExternalXorCodec.name

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case ExternalXorCodec(mask, _) =>
      DecodedLength.validate(transform(encoded, mask), expectedDecoded, limits)
    case found => wrongCodec(found)

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case ExternalXorCodec(mask, _) => Right(transform(decoded, mask))
    case found => wrongCodec(found)

  private def wrongCodec(found: CompiledCodec): Left[CodecError, Nothing] =
    Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

object ExternalXorAsyncExecutor extends AsyncByteCodecExecutor:
  val name = ExternalXorCodec.name

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(codec match
      case ExternalXorCodec(mask, _) =>
        DecodedLength.validate(transform(encoded, mask), expectedDecoded, limits)
      case found => Left(CodecError.CorruptData(
        name,
        s"executor received compiled codec ${found.name}"
      ))
    )

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(codec match
      case ExternalXorCodec(mask, _) => Right(transform(decoded, mask))
      case found => Left(CodecError.CorruptData(
        name,
        s"executor received compiled codec ${found.name}"
      ))
    )

private def transform(bytes: OwnedBytes, mask: Int): OwnedBytes =
  val result = bytes.toArray
  var index = 0
  while index < result.length do
    result(index) = (result(index) ^ mask).toByte
    index += 1
  OwnedBytes.copyOf(result)

object ExternalXorFixture:
  val capabilities = ZarrCapabilities(
    codecs = BuiltInCodecs.all :+ ExternalXorCapability
  )
  val syncRuntime: SyncCodecRuntime = value(SyncCodecRuntime(
    "external test runtime",
    Vector(ExternalXorSyncExecutor)
  ))
  val asyncRuntime: AsyncCodecRuntime = value(AsyncCodecRuntime(
    "external browser test runtime",
    Vector(ExternalXorAsyncExecutor)
  ))

  val directMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"org.scalafim.test.xor","configuration":{"mask":90}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  val shardedMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"sharding_indexed","configuration":{"chunk_shape":[2,2],"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"org.scalafim.test.xor","configuration":{"mask":90}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"crc32c"}],"index_location":"start"}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  val directChunk: OwnedBytes = xor(int16(1, 2, 3, 4, 5, 6))

  val shardedChunks: Vector[PrimitiveBlock] = Vector(
    int16Block(1, 2, 5, 6),
    int16Block(3, 4, 7, 8),
    int16Block(9, 10, 13, 14),
    int16Block(11, 12, 15, 16)
  )

  val shardedObject: OwnedBytes =
    val encoded = shardedChunks.map: block =>
      xor(codecValue(ScalarBytes.encode(block, BuiltInDataTypes.int16, Some(Endianness.Little))))
    val innerGrid = value(Shape(2L, 2L))
    val indexLength = value(ShardIndexCodec.encodedLength(innerGrid)).toLong
    var offset = indexLength
    val entries = encoded.map: chunk =>
      val entry = ShardIndexEntry.Present(offset, chunk.byteCount)
      offset += chunk.byteCount.toLong
      entry
    val index = value(ShardIndex(innerGrid, entries))
    val indexBytes = value(ShardIndexCodec.encode(index))
    OwnedBytes.copyOf(indexBytes.toArray ++ encoded.flatMap(_.toArray))

  val fullValues: Vector[Short] = (1 to 16).map(_.toShort).toVector

  def directObjects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> utf8(directMetadata),
    "c/0/0" -> directChunk
  )

  def shardedObjects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> utf8(shardedMetadata),
    "c/0/0" -> shardedObject
  )

  def descriptor(metadata: String): Either[ZarrError, ArrayDescriptor] =
    ZarrMetadata.parse(metadata).flatMap:
      case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array, capabilities)
      case ZarrNodeMetadata.Group(_) => Left(ZarrError.UnsupportedNodeType("group"))

  private def int16(values: Short*): OwnedBytes =
    codecValue(ScalarBytes.encode(
      int16Block(values*),
      BuiltInDataTypes.int16,
      Some(Endianness.Little)
    ))

  private def int16Block(values: Short*): PrimitiveBlock =
    PrimitiveBlock.Int16(OwnedShorts.copyOf(values.toArray))

  private def xor(bytes: OwnedBytes): OwnedBytes = transform(bytes, 90)

  private def utf8(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => throw new IllegalArgumentException(error.message)

  private def codecValue[A](result: Either[CodecError, A]): A = result match
    case Right(found) => found
    case Left(error) => throw new IllegalArgumentException(error.message)
