package zarr4s

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import scala.util.control.NonFatal

object JvmGzip extends SyncByteCodecExecutor:
  val name = "gzip"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case _: GzipCodec => decode(encoded, expectedDecoded, limits)
    case found        =>
      Left(
        CodecError.CorruptData(
          "gzip",
          s"executor received compiled codec ${found.name}"
        )
      )

  override def decodeBounded(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case _: GzipCodec => decodeBounded(encoded, limits)
    case found        =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case GzipCodec(level) => encode(decoded, level)
    case found            =>
      Left(
        CodecError.CorruptData(
          "gzip",
          s"executor received compiled codec ${found.name}"
        )
      )

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits = DecodeLimits.default
  ): Either[CodecError, OwnedBytes] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Left(
        CodecError.DecodedLimitExceeded(
          limits.maxDecodedBytes.toLong,
          expectedDecoded.toLong
        )
      )
    else if expectedDecoded.toLong > Int.MaxValue.toLong then
      Left(CodecError.DecodedLimitExceeded(Int.MaxValue, expectedDecoded.toLong))
    else
      try
        val input = new GZIPInputStream(new ByteArrayInputStream(encoded.values))
        val output = new ByteArrayOutputStream(math.min(expectedDecoded.toLong, 65536L).toInt)
        val buffer = new Array[Byte](8192)
        var total = 0L
        var read = input.read(buffer)
        while read >= 0 do
          if read > 0 then
            total += read.toLong
            if total > expectedDecoded.toLong then
              input.close()
              return Left(CodecError.InvalidDecodedLength(expectedDecoded.toLong, total))
            output.write(buffer, 0, read)
          read = input.read(buffer)
        input.close()
        DecodedLength.validate(
          OwnedBytes.unsafe(output.toByteArray),
          expectedDecoded,
          limits
        )
      catch case NonFatal(error) => Left(CodecError.CorruptData("gzip", error.getMessage))

  /** Decode a gzip stream while bounding the materialized expansion. */
  def decodeBounded(
      encoded: OwnedBytes,
      limits: DecodeLimits = DecodeLimits.default
  ): Either[CodecError, OwnedBytes] =
    val limit = math.min(limits.maxDecodedBytes.toLong, Int.MaxValue.toLong)
    try
      val input = new GZIPInputStream(new ByteArrayInputStream(encoded.values))
      val output = new ByteArrayOutputStream(math.min(limit, 65536L).toInt)
      val buffer = new Array[Byte](8192)
      var total = 0L
      var read = input.read(buffer)
      while read >= 0 do
        if read > 0 then
          total += read.toLong
          if total > limit then
            input.close()
            return Left(CodecError.DecodedLimitExceeded(limit, total))
          output.write(buffer, 0, read)
        read = input.read(buffer)
      input.close()
      Right(OwnedBytes.unsafe(output.toByteArray))
    catch case NonFatal(error) => Left(CodecError.CorruptData("gzip", error.getMessage))

  def encode(decoded: OwnedBytes, level: Int = 1): Either[CodecError, OwnedBytes] =
    if level < 0 || level > 9 then
      Left(CodecError.CorruptData("gzip", s"level must be in [0, 9], found $level"))
    else
      try
        val output = new ByteArrayOutputStream()
        val gzip = new LevelGzipOutputStream(output, level)
        gzip.write(decoded.values)
        gzip.finish()
        gzip.close()
        Right(OwnedBytes.unsafe(output.toByteArray))
      catch case NonFatal(error) => Left(CodecError.CorruptData("gzip", error.getMessage))

  private final class LevelGzipOutputStream(
      output: ByteArrayOutputStream,
      level: Int
  ) extends GZIPOutputStream(output):
    `def`.setLevel(level)

object JvmCodecRuntime:
  val portable: SyncCodecRuntime =
    SyncCodecRuntime.unsafe("JVM", Vector(JvmGzip, JvmZlib))
