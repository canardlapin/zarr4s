package zarr4s.codec.blosc

import zarr4s.*
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport
import scala.scalajs.js.typedarray.Uint8Array
import scala.util.control.NonFatal

object BrowserBloscZstd extends AsyncByteCodecExecutor:
  val name = "blosc"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case found: BloscZstdCodec =>
      BloscFrame.validate(found, encoded, expectedDecoded, limits) match
        case Left(error) => Future.successful(Left(error))
        case Right(_)    =>
          transform(found, encoded, encode = false).map:
            case Left(error)    => Left(error)
            case Right(decoded) => DecodedLength.validate(decoded, expectedDecoded, limits)
    case found =>
      Future.successful(
        Left(
          CodecError.CorruptData(
            "blosc",
            s"executor received compiled codec ${found.name}"
          )
        )
      )

  /** Encode using numcodecs.js. Its Blosc binding fixes `typesize` at four and does not expose it,
    * so accepting another stride here would silently write metadata-inconsistent shuffle frames.
    */
  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case found: BloscZstdCodec if found.typeSize.toInt != 4 =>
      Future.successful(
        Left(
          CodecError.UnsupportedCapability(
            s"blosc encode with typesize ${found.typeSize.toInt}",
            "Scala.js numcodecs"
          )
        )
      )
    case found: BloscZstdCodec =>
      transform(found, decoded, encode = true).map:
        case Left(error)    => Left(error)
        case Right(encoded) =>
          BloscFrame
            .validate(
              found,
              encoded,
              decoded.byteCount,
              DecodeLimits(decoded.byteCount)
            )
            .map(_ => encoded)
    case found =>
      Future.successful(
        Left(
          CodecError.CorruptData(
            "blosc",
            s"executor received compiled codec ${found.name}"
          )
        )
      )

  private def transform(
      codec: BloscZstdCodec,
      input: OwnedBytes,
      encode: Boolean
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    try
      val algorithm = NumcodecsBlosc.fromConfig(
        js.Dynamic.literal(
          id = "blosc",
          cname = "zstd",
          clevel = codec.compressionLevel.toInt,
          shuffle = codec.shuffle.nativeCode,
          blocksize = codec.blockSize.toInt
        )
      )
      val source = toUint8Array(input)
      val promise = if encode then algorithm.encode(source) else algorithm.decode(source)
      promise.toFuture
        .map(output => Right(fromUint8Array(output)))
        .recover:
          case NonFatal(error) => Left(corrupt(error))
    catch case NonFatal(error) => Future.successful(Left(corrupt(error)))

  private def toUint8Array(bytes: OwnedBytes): Uint8Array =
    val result = new Uint8Array(bytes.length)
    var index = 0
    while index < bytes.length do
      result(index) = bytes(index)
      index += 1
    result

  private def fromUint8Array(bytes: Uint8Array): OwnedBytes =
    val result = new Array[Byte](bytes.length)
    var index = 0
    while index < result.length do
      result(index) = bytes(index).toByte
      index += 1
    OwnedBytes.unsafe(result)

  private def corrupt(error: Throwable): CodecError =
    val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    CodecError.CorruptData("blosc", detail)

@js.native
@JSImport("numcodecs/blosc", "default")
private object NumcodecsBlosc extends js.Object:
  def fromConfig(configuration: js.Object): NumcodecsBloscCodec = js.native

@js.native
private trait NumcodecsBloscCodec extends js.Object:
  def encode(input: Uint8Array): js.Promise[Uint8Array] = js.native
  def decode(input: Uint8Array): js.Promise[Uint8Array] = js.native

object BrowserBloscZstdRuntime:
  val portable: AsyncCodecRuntime = AsyncCodecRuntime(
    "Scala.js with Blosc/Zstd",
    Vector(BrowserGzip, BrowserZlib, BrowserBloscZstd)
  ) match
    case Right(found) => found
    case Left(error)  => throw new IllegalStateException(error.message)
