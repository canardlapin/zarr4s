package zarr4s.codec.blosc

import zarr4s.*

opaque type ZstdCompressionLevel = Int

object ZstdCompressionLevel:
  def apply(value: Int): Either[String, ZstdCompressionLevel] =
    if value >= -131072 && value <= 22 then Right(value)
    else Left(s"zstd level must be in [-131072, 22], found $value")

  extension (level: ZstdCompressionLevel) inline def toInt: Int = level

final case class ZstdCodec private (
    compressionLevel: ZstdCompressionLevel,
    checksum: Boolean,
    configuration: JsonObject
) extends CompiledCodec:
  val name = "zstd"
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes

object ZstdCodec:
  def create(level: Int, checksum: Boolean): Either[String, ZstdCodec] =
    ZstdCompressionLevel(level).map: validLevel =>
      new ZstdCodec(
        validLevel,
        checksum,
        JsonObject.unsafe(
          Vector(
            "level" -> JsonValue.Num(JsonNumber.unsafe(level.toString)),
            "checksum" -> JsonValue.Bool(checksum)
          )
        )
      )

object ZstdCapability extends CodecCapability:
  val name = "zstd"

  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec] =
    for
      level <- optionalInt(extension.configuration, "level", 0)
      checksum <- optionalBoolean(extension.configuration, "checksum", false)
      codec <- ZstdCodec.create(level, checksum)
    yield codec

  private def optionalInt(
      configuration: JsonObject,
      field: String,
      default: Int
  ): Either[String, Int] = configuration.get(field) match
    case None                        => Right(default)
    case Some(JsonValue.Num(number)) =>
      number.toLongExact.flatMap: found =>
        if found < Int.MinValue.toLong || found > Int.MaxValue.toLong then
          Left(s"zstd $field is outside the Int range: $found")
        else Right(found.toInt)
    case Some(_) => Left(s"zstd $field must be an integer")

  private def optionalBoolean(
      configuration: JsonObject,
      field: String,
      default: Boolean
  ): Either[String, Boolean] = configuration.get(field) match
    case None                        => Right(default)
    case Some(JsonValue.Bool(value)) => Right(value)
    case Some(_)                     => Left(s"zstd $field must be a boolean")

object ZstdProvider:
  def capabilities(base: ZarrCapabilities = ZarrCapabilities()): ZarrCapabilities =
    base.copy(codecs = base.codecs.filterNot(_.name == ZstdCapability.name) :+ ZstdCapability)
