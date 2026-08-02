package zarr4s.codec.blosc

import com.github.luben.zstd.Zstd
import scala.util.control.NonFatal
import zarr4s.*

object JvmZstd extends SyncByteCodecExecutor:
  val name = "zstd"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case _: ZstdCodec => decode(encoded, expectedDecoded, limits)
    case found        =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case found: ZstdCodec => encode(decoded, found)
    case found            =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits = DecodeLimits.default
  ): Either[CodecError, OwnedBytes] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, expectedDecoded.toLong))
    else if expectedDecoded.toLong > Int.MaxValue.toLong then
      Left(CodecError.DecodedLimitExceeded(Int.MaxValue.toLong, expectedDecoded.toLong))
    else
      try
        val output = new Array[Byte](expectedDecoded.toLong.toInt)
        val actual = Zstd.decompress(output, encoded.toArray)
        if Zstd.isError(actual) then Left(corrupt(Zstd.getErrorName(actual)))
        else DecodedLength.validate(OwnedBytes.unsafe(output), expectedDecoded, limits)
      catch case NonFatal(error) => Left(corrupt(error))

  override def decodeBounded(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case found: ZstdCodec =>
      try
        val declared = Zstd.getFrameContentSize(encoded.toArray)
        if declared <= 0L then
          Left(CodecError.CorruptData(name, "zstd frame does not declare a decoded size"))
        else if declared > limits.maxDecodedBytes.toLong then
          Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, declared))
        else if declared > Int.MaxValue.toLong then
          Left(CodecError.DecodedLimitExceeded(Int.MaxValue.toLong, declared))
        else decode(found, encoded, ByteCount.unsafe(declared), limits)
      catch case NonFatal(error) => Left(corrupt(error))
    case found =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

  def encode(decoded: OwnedBytes, codec: ZstdCodec): Either[CodecError, OwnedBytes] =
    try
      val source = decoded.toArray
      val bound = Zstd.compressBound(source.length.toLong)
      if bound > Int.MaxValue.toLong then
        Left(CodecError.CorruptData(name, s"compression bound exceeds Int range: $bound"))
      else
        val output = new Array[Byte](bound.toInt)
        val written = Zstd.compress(
          output,
          source,
          codec.compressionLevel.toInt,
          codec.checksum
        )
        if Zstd.isError(written) then Left(corrupt(Zstd.getErrorName(written)))
        else Right(OwnedBytes.copyOf(java.util.Arrays.copyOf(output, written.toInt)))
    catch case NonFatal(error) => Left(corrupt(error))

  private def corrupt(error: Throwable): CodecError =
    val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    CodecError.CorruptData(name, detail)

  private def corrupt(detail: String): CodecError = CodecError.CorruptData(name, detail)
