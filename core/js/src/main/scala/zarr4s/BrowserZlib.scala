package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Scala.js executor for the zlib-wrapped DEFLATE stream used by common Zarr v2 stores. */
object BrowserZlib extends AsyncByteCodecExecutor:
  val name = "zlib"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: ZlibCodec => decode(encoded, expectedDecoded, limits)
    case found        =>
      Future.successful(
        Left(CodecError.CorruptData("zlib", s"executor received compiled codec ${found.name}"))
      )

  override def decodeBounded(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: ZlibCodec =>
      BrowserCompressionStreams
        .transform("zlib", "deflate", "DecompressionStream", encoded)
        .map:
          case Left(error) => Left(error)
          case Right(decoded) if decoded.byteCount.toLong > limits.maxDecodedBytes.toLong =>
            Left(
              CodecError.DecodedLimitExceeded(
                limits.maxDecodedBytes.toLong,
                decoded.byteCount.toLong
              )
            )
          case Right(decoded) => Right(decoded)
    case found =>
      Future.successful(
        Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))
      )

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: ZlibCodec => encode(decoded)
    case found        =>
      Future.successful(
        Left(CodecError.CorruptData("zlib", s"executor received compiled codec ${found.name}"))
      )

  def available: Boolean = BrowserCompressionStreams.available("deflate")

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits = DecodeLimits.default
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Future.successful(
        Left(
          CodecError.DecodedLimitExceeded(
            limits.maxDecodedBytes.toLong,
            expectedDecoded.toLong
          )
        )
      )
    else
      BrowserCompressionStreams
        .transform("zlib", "deflate", "DecompressionStream", encoded)
        .map:
          case Left(error)    => Left(error)
          case Right(decoded) => DecodedLength.validate(decoded, expectedDecoded, limits)

  def encode(
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    BrowserCompressionStreams.transform("zlib", "deflate", "CompressionStream", decoded)
