package zarr4s.codec.blosc

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport
import scala.scalajs.js.typedarray.Uint8Array
import scala.util.control.NonFatal
import zarr4s.*

object BrowserZstd extends AsyncByteCodecExecutor:
  val name = "zstd"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: ZstdCodec => decode(encoded, expectedDecoded, limits)
    case found        =>
      Future.successful(
        Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))
      )

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case found: ZstdCodec => encode(decoded, found)
    case found            =>
      Future.successful(
        Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))
      )

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits = DecodeLimits.default
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Future.successful(
        Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, expectedDecoded.toLong))
      )
    else if expectedDecoded.toLong > Int.MaxValue.toLong then
      Future.successful(
        Left(CodecError.DecodedLimitExceeded(Int.MaxValue.toLong, expectedDecoded.toLong))
      )
    else
      Future:
        try
          val output = ZstdifyDecompress(
            toUint8Array(encoded),
            js.Dynamic.literal(maxSize = expectedDecoded.toLong.toDouble)
          )
          DecodedLength.validate(fromUint8Array(output), expectedDecoded, limits)
        catch case NonFatal(error) => Left(corrupt(error))

  def encode(
      decoded: OwnedBytes,
      codec: ZstdCodec
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = Future:
    try
      val output = ZstdifyCompress(
        toUint8Array(decoded),
        js.Dynamic.literal(
          checksum = codec.checksum,
          level = codec.compressionLevel.toInt
        )
      )
      Right(fromUint8Array(output))
    catch case NonFatal(error) => Left(corrupt(error))

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
    while index < bytes.length do
      result(index) = bytes(index).toByte
      index += 1
    OwnedBytes.unsafe(result)

  private def corrupt(error: Throwable): CodecError =
    val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    CodecError.CorruptData(name, detail)

@js.native
@JSImport("zstdify", "compress")
private object ZstdifyCompress extends js.Object:
  def apply(input: Uint8Array, options: js.Object): Uint8Array = js.native

@js.native
@JSImport("zstdify", "decompress")
private object ZstdifyDecompress extends js.Object:
  def apply(input: Uint8Array, options: js.Object): Uint8Array = js.native
