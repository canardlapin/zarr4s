package scalafim.zarr.codec.blosc

import scalafim.zarr.*

enum BloscShuffle(val metadataName: String, val nativeCode: Int):
  case NoShuffle extends BloscShuffle("noshuffle", 0)
  case ByteShuffle extends BloscShuffle("shuffle", 1)
  case BitShuffle extends BloscShuffle("bitshuffle", 2)

object BloscShuffle:
  def parse(value: String): Either[String, BloscShuffle] = value match
    case "noshuffle" => Right(BloscShuffle.NoShuffle)
    case "shuffle" => Right(BloscShuffle.ByteShuffle)
    case "bitshuffle" => Right(BloscShuffle.BitShuffle)
    case found => Left(
      s"blosc shuffle must be 'noshuffle', 'shuffle', or 'bitshuffle', found '$found'"
    )

opaque type BloscCompressionLevel = Int

object BloscCompressionLevel:
  def apply(value: Int): Either[String, BloscCompressionLevel] =
    if value >= 0 && value <= 9 then Right(value)
    else Left(s"blosc clevel must be in [0, 9], found $value")

  extension (level: BloscCompressionLevel)
    inline def toInt: Int = level

opaque type BloscTypeSize = Int

object BloscTypeSize:
  def apply(value: Int): Either[String, BloscTypeSize] =
    if value >= 1 && value <= 255 then Right(value)
    else Left(s"blosc typesize must be in [1, 255], found $value")

  extension (size: BloscTypeSize)
    inline def toInt: Int = size

opaque type BloscBlockSize = Int

object BloscBlockSize:
  def apply(value: Int): Either[String, BloscBlockSize] =
    if value >= 0 then Right(value)
    else Left(s"blosc blocksize must be non-negative, found $value")

  extension (size: BloscBlockSize)
    inline def toInt: Int = size

final case class BloscZstdCodec private (
    compressionLevel: BloscCompressionLevel,
    shuffle: BloscShuffle,
    typeSize: BloscTypeSize,
    blockSize: BloscBlockSize,
    configuration: JsonObject
) extends CompiledCodec:
  val name = "blosc"
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes

object BloscZstdCodec:
  def create(
      compressionLevel: Int,
      shuffle: BloscShuffle,
      typeSize: Int,
      blockSize: Int
  ): Either[String, BloscZstdCodec] =
    for
      level <- BloscCompressionLevel(compressionLevel)
      stride <- BloscTypeSize(typeSize)
      block <- BloscBlockSize(blockSize)
    yield new BloscZstdCodec(
      level,
      shuffle,
      stride,
      block,
      canonicalConfiguration(level, shuffle, stride, block)
    )

  private def canonicalConfiguration(
      level: BloscCompressionLevel,
      shuffle: BloscShuffle,
      typeSize: BloscTypeSize,
      blockSize: BloscBlockSize
  ): JsonObject = JsonObject.unsafe(Vector(
    "cname" -> JsonValue.Str("zstd"),
    "clevel" -> integer(level.toInt),
    "shuffle" -> JsonValue.Str(shuffle.metadataName),
    "typesize" -> integer(typeSize.toInt),
    "blocksize" -> integer(blockSize.toInt)
  ))

  private def integer(value: Int): JsonValue =
    JsonValue.Num(JsonNumber.unsafe(value.toString))

object BloscZstdCapability extends CodecCapability:
  val name = "blosc"

  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec] =
    for
      cname <- requiredString(extension.configuration, "cname")
      _ <- if cname == "zstd" then Right(())
        else Left(s"blosc-zstd provider requires cname 'zstd', found '$cname'")
      level <- requiredInt(extension.configuration, "clevel")
      shuffleName <- requiredString(extension.configuration, "shuffle")
      shuffle <- BloscShuffle.parse(shuffleName)
      typeSize <- optionalInt(extension.configuration, "typesize").flatMap:
        case Some(found) => Right(found)
        case None if shuffle == BloscShuffle.NoShuffle => Right(1)
        case None => Left("blosc typesize is required when shuffle is enabled")
      blockSize <- requiredInt(extension.configuration, "blocksize")
      codec <- BloscZstdCodec.create(level, shuffle, typeSize, blockSize)
    yield codec

  private def requiredString(value: JsonObject, field: String): Either[String, String] =
    value.get(field) match
      case Some(JsonValue.Str(found)) => Right(found)
      case Some(_) => Left(s"blosc $field must be a string")
      case None => Left(s"missing required blosc field '$field'")

  private def requiredInt(value: JsonObject, field: String): Either[String, Int] =
    optionalInt(value, field).flatMap:
      case Some(found) => Right(found)
      case None => Left(s"missing required blosc field '$field'")

  private def optionalInt(value: JsonObject, field: String): Either[String, Option[Int]] =
    value.get(field) match
      case Some(JsonValue.Num(number)) => number.toLongExact.flatMap: found =>
        if found < Int.MinValue.toLong || found > Int.MaxValue.toLong then
          Left(s"blosc $field is outside the Int range: $found")
        else Right(Some(found.toInt))
      case Some(_) => Left(s"blosc $field must be an integer")
      case None => Right(None)

object BloscZstdProvider:
  def capabilities(base: ZarrCapabilities = ZarrCapabilities()): ZarrCapabilities =
    base.copy(codecs = base.codecs.filterNot(_.name == BloscZstdCapability.name) :+ BloscZstdCapability)

private[blosc] object BloscFrame:
  private val HeaderBytes = 16

  def validate(
      codec: BloscZstdCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, Unit] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Left(CodecError.DecodedLimitExceeded(
        limits.maxDecodedBytes.toLong,
        expectedDecoded.toLong
      ))
    else if expectedDecoded.toLong > Int.MaxValue.toLong then
      Left(CodecError.DecodedLimitExceeded(Int.MaxValue.toLong, expectedDecoded.toLong))
    else if encoded.length < HeaderBytes then
      corrupt(s"header requires $HeaderBytes bytes, found ${encoded.length}")
    else
      val decodedLength = littleEndianUInt32(encoded, 4)
      val blockSize = littleEndianUInt32(encoded, 8)
      val encodedLength = littleEndianUInt32(encoded, 12)
      val frameTypeSize = encoded(3).toInt & 0xff
      if decodedLength != expectedDecoded.toLong then
        Left(CodecError.InvalidDecodedLength(expectedDecoded.toLong, decodedLength))
      else if encodedLength != encoded.length.toLong then
        Left(CodecError.InvalidEncodedLength(encodedLength, encoded.length.toLong))
      else if blockSize > decodedLength && decodedLength != 0L then
        corrupt(s"block size $blockSize exceeds decoded length $decodedLength")
      else if frameTypeSize == 0 then corrupt("typesize is zero")
      else if codec.shuffle != BloscShuffle.NoShuffle && frameTypeSize != codec.typeSize.toInt then
        corrupt(
          s"typesize $frameTypeSize does not match metadata ${codec.typeSize.toInt}"
        )
      else Right(())

  private def littleEndianUInt32(bytes: OwnedBytes, offset: Int): Long =
    var result = 0L
    var index = 0
    while index < 4 do
      result |= (bytes(offset + index).toLong & 0xffL) << (index * 8)
      index += 1
    result

  private def corrupt(detail: String): Left[CodecError, Nothing] =
    Left(CodecError.CorruptData("blosc", detail))
