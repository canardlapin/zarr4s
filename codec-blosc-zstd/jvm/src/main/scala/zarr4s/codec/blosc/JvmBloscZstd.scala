package zarr4s.codec.blosc

import com.scalableminds.bloscjava.Blosc
import zarr4s.*
import scala.util.control.NonFatal

object JvmBloscZstd extends SyncByteCodecExecutor:
  val name = "blosc"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case found: BloscZstdCodec =>
      BloscFrame
        .validate(found, encoded, expectedDecoded, limits)
        .flatMap: _ =>
          try
            DecodedLength.validate(
              OwnedBytes.unsafe(Blosc.decompress(encoded.toArray)),
              expectedDecoded,
              limits
            )
          catch case NonFatal(error) => Left(corrupt(error))
    case found =>
      Left(
        CodecError.CorruptData(
          "blosc",
          s"executor received compiled codec ${found.name}"
        )
      )

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case found: BloscZstdCodec =>
      try
        Right(
          OwnedBytes.unsafe(
            Blosc.compress(
              decoded.toArray,
              found.typeSize.toInt,
              Blosc.Compressor.ZSTD,
              found.compressionLevel.toInt,
              nativeShuffle(found.shuffle),
              found.blockSize.toInt,
              1
            )
          )
        )
      catch case NonFatal(error) => Left(corrupt(error))
    case found =>
      Left(
        CodecError.CorruptData(
          "blosc",
          s"executor received compiled codec ${found.name}"
        )
      )

  private def nativeShuffle(shuffle: BloscShuffle): Blosc.Shuffle = shuffle match
    case BloscShuffle.NoShuffle   => Blosc.Shuffle.NO_SHUFFLE
    case BloscShuffle.ByteShuffle => Blosc.Shuffle.BYTE_SHUFFLE
    case BloscShuffle.BitShuffle  => Blosc.Shuffle.BIT_SHUFFLE

  private def corrupt(error: Throwable): CodecError =
    val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    CodecError.CorruptData("blosc", detail)

object JvmBloscZstdRuntime:
  val portable: SyncCodecRuntime = SyncCodecRuntime(
    "JVM with Blosc/Zstd",
    Vector(JvmGzip, JvmZlib, JvmZstd, JvmBloscZstd)
  ) match
    case Right(found) => found
    case Left(error)  => throw new IllegalStateException(error.message)
