package scalafim.zarr

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.typedarray.ArrayBuffer
import scala.scalajs.js.typedarray.Uint8Array
import scala.util.control.NonFatal

object BrowserGzip extends AsyncByteCodecExecutor:
  val name = "gzip"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: GzipCodec => decode(encoded, expectedDecoded, limits)
    case found => Future.successful(Left(CodecError.CorruptData(
      "gzip",
      s"executor received compiled codec ${found.name}"
    )))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case _: GzipCodec => encode(decoded)
    case found => Future.successful(Left(CodecError.CorruptData(
      "gzip",
      s"executor received compiled codec ${found.name}"
    )))

  def available: Boolean =
    !js.isUndefined(js.Dynamic.global.DecompressionStream) &&
      !js.isUndefined(js.Dynamic.global.CompressionStream)

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits = DecodeLimits.default
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Future.successful(Left(CodecError.DecodedLimitExceeded(
        limits.maxDecodedBytes.toLong,
        expectedDecoded.toLong
      )))
    else transform("DecompressionStream", encoded).map:
      case Left(error) => Left(error)
      case Right(decoded) => DecodedLength.validate(decoded, expectedDecoded, limits)

  def encode(
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    transform("CompressionStream", decoded)

  private def transform(
      constructorName: String,
      input: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    val constructor =
      if constructorName == "DecompressionStream" then js.Dynamic.global.DecompressionStream
      else js.Dynamic.global.CompressionStream
    if js.isUndefined(constructor) then
      Future.successful(Left(CodecError.UnsupportedCapability("gzip", "browser")))
    else
      try
        val source = new Uint8Array(input.length)
        var index = 0
        while index < input.length do
          source(index) = input(index)
          index += 1
        val blobParts = js.Array(source.buffer)
        val blob = js.Dynamic.newInstance(js.Dynamic.global.Blob)(blobParts)
        val codec = js.Dynamic.newInstance(constructor)("gzip")
        val stream = blob.stream().pipeThrough(codec)
        val response = js.Dynamic.newInstance(js.Dynamic.global.Response)(stream)
        val promise = response.arrayBuffer().asInstanceOf[js.Promise[ArrayBuffer]]
        promise.toFuture.map: buffer =>
          val view = new Uint8Array(buffer)
          val output = new Array[Byte](view.length)
          var outputIndex = 0
          while outputIndex < output.length do
            output(outputIndex) = view(outputIndex).toByte
            outputIndex += 1
          Right(OwnedBytes.unsafe(output))
        .recover:
          case NonFatal(error) => Left(CodecError.CorruptData("gzip", error.getMessage))
      catch
        case NonFatal(error) =>
          Future.successful(Left(CodecError.CorruptData("gzip", error.getMessage)))

object BrowserCodecRuntime:
  val portable: AsyncCodecRuntime =
    AsyncCodecRuntime.unsafe("browser", Vector(BrowserGzip))
