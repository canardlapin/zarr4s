package zarr4s

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream
import scala.util.control.NonFatal

/** JVM executor for the zlib-wrapped DEFLATE stream used by common Zarr v2 stores. */
object JvmZlib extends SyncByteCodecExecutor:
  val name = "zlib"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case _: ZlibCodec => decode(encoded, expectedDecoded, limits)
    case found        =>
      Left(CodecError.CorruptData("zlib", s"executor received compiled codec ${found.name}"))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case ZlibCodec(level) => encode(decoded, level)
    case found            =>
      Left(CodecError.CorruptData("zlib", s"executor received compiled codec ${found.name}"))

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
      val inflater = new Inflater()
      val input = new InflaterInputStream(
        new ByteArrayInputStream(encoded.values),
        inflater,
        8192
      )
      try
        val output = new ByteArrayOutputStream(math.min(expectedDecoded.toLong, 65536L).toInt)
        val buffer = new Array[Byte](8192)
        var total = 0L
        var read = input.read(buffer)
        while read >= 0 do
          if read > 0 then
            total += read.toLong
            if total > expectedDecoded.toLong then
              return Left(CodecError.InvalidDecodedLength(expectedDecoded.toLong, total))
            output.write(buffer, 0, read)
          read = input.read(buffer)
        if !inflater.finished() then
          Left(
            CodecError
              .CorruptData("zlib", "compressed stream ended before the DEFLATE stream finished")
          )
        else
          DecodedLength.validate(
            OwnedBytes.unsafe(output.toByteArray),
            expectedDecoded,
            limits
          )
      catch case NonFatal(error) => Left(CodecError.CorruptData("zlib", error.getMessage))
      finally
        try input.close()
        catch case NonFatal(_) => ()
        inflater.end()

  def encode(decoded: OwnedBytes, level: Int = 1): Either[CodecError, OwnedBytes] =
    if level < -1 || level > 9 then
      Left(CodecError.CorruptData("zlib", s"level must be in [-1, 9], found $level"))
    else
      val output = new ByteArrayOutputStream()
      val deflater = new Deflater(level)
      val stream = new DeflaterOutputStream(output, deflater, 8192)
      try
        stream.write(decoded.values)
        stream.finish()
        Right(OwnedBytes.unsafe(output.toByteArray))
      catch case NonFatal(error) => Left(CodecError.CorruptData("zlib", error.getMessage))
      finally
        try stream.close()
        catch case NonFatal(_) => ()
        deflater.end()
