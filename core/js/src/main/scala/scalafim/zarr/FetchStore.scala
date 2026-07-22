package scalafim.zarr

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.typedarray.ArrayBuffer
import scala.scalajs.js.typedarray.Uint8Array
import scala.util.control.NonFatal

final class FetchStore private (
    baseUrl: String,
    private val requests: mutable.ArrayBuffer[ObjectRequest]
)(using ExecutionContext) extends AsyncObjectReader:
  def read(key: StoreKey, range: ByteRange): Future[Either[StoreError, OwnedBytes]] =
    requests += ObjectRequest.Range(key, range)
    if range.length.toLong == 0L then Future.successful(Right(OwnedBytes.empty))
    else
      val headers = js.Dictionary("Range" -> rangeHeader(range))
      fetch(key, "GET", headers).flatMap:
        case Left(error) => Future.successful(Left(error))
        case Right(response) =>
          response.status.asInstanceOf[Int] match
            case 206 =>
              val header = response.headers.get("Content-Range").asInstanceOf[String | Null]
              val expectedEnd = range.offset + range.length.toLong - 1L
              if header == null || !matchesContentRange(header, range.offset, expectedEnd) then
                cancel(response)
                Future.successful(Left(StoreError.Transport(
                  key,
                  "invalid Content-Range response",
                  transient = false
                )))
              else body(key, response, range.length).map:
                case Right(bytes) if bytes.byteCount == range.length => Right(bytes)
                case Right(bytes) => Left(StoreError.Transport(
                  key,
                  s"range body length ${bytes.length} does not match ${range.length.toLong}",
                  transient = false
                ))
                case Left(error) => Left(error)
            case 200 =>
              cancel(response)
              Future.successful(Left(StoreError.RangeIgnored(key, range)))
            case status =>
              cancel(response)
              Future.successful(Left(statusError(key, status)))

  def readAll(key: StoreKey, maxBytes: ByteCount): Future[Either[StoreError, OwnedBytes]] =
    requests += ObjectRequest.Whole(key)
    fetch(key, "GET", js.Dictionary.empty).flatMap:
      case Left(error) => Future.successful(Left(error))
      case Right(response) =>
        val status = response.status.asInstanceOf[Int]
        if status < 200 || status >= 300 then
          cancel(response)
          Future.successful(Left(statusError(key, status)))
        else
          contentLength(response) match
            case None =>
              cancel(response)
              Future.successful(Left(StoreError.ObjectLengthUnavailable(key)))
            case Some(length) if length > maxBytes.toLong =>
              cancel(response)
              Future.successful(Left(StoreError.ObjectTooLarge(key, maxBytes.toLong, length)))
            case Some(length) => body(key, response, ByteCount.unsafe(length))

  def length(key: StoreKey): Future[Either[StoreError, Long]] =
    requests += ObjectRequest.Length(key)
    fetch(key, "HEAD", js.Dictionary.empty).map:
      case Left(error) => Left(error)
      case Right(response) =>
        val status = response.status.asInstanceOf[Int]
        cancel(response)
        if status >= 200 && status < 300 then
          contentLength(response).toRight(StoreError.ObjectLengthUnavailable(key))
        else Left(statusError(key, status))

  def trace: Vector[ObjectRequest] = requests.toVector

  def clearTrace(): Unit = requests.clear()

  private def fetch(
      key: StoreKey,
      method: String,
      headers: js.Dictionary[String]
  ): Future[Either[StoreError, js.Dynamic]] =
    try
      val init = js.Dynamic.literal(
        method = method,
        headers = headers
      )
      js.Dynamic.global.fetch(s"$baseUrl${key.value}", init)
        .asInstanceOf[js.Promise[js.Dynamic]]
        .toFuture
        .map(Right.apply)
        .recover:
          case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = true))
    catch
      case NonFatal(error) =>
        Future.successful(Left(StoreError.Transport(key, error.getMessage, transient = true)))

  private def body(
      key: StoreKey,
      response: js.Dynamic,
      expected: ByteCount
  ): Future[Either[StoreError, OwnedBytes]] =
    if expected.toLong > Int.MaxValue.toLong then
      cancel(response)
      Future.successful(Left(StoreError.ObjectTooLarge(key, Int.MaxValue, expected.toLong)))
    else
      response.arrayBuffer().asInstanceOf[js.Promise[ArrayBuffer]].toFuture
        .map: buffer =>
          val view = new Uint8Array(buffer)
          if view.length.toLong > expected.toLong then
            Left(StoreError.ObjectTooLarge(key, expected.toLong, view.length.toLong))
          else
            val bytes = new Array[Byte](view.length)
            var index = 0
            while index < bytes.length do
              bytes(index) = view(index).toByte
              index += 1
            Right(OwnedBytes.unsafe(bytes))
        .recover:
          case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = true))

  private def cancel(response: js.Dynamic): Unit =
    if response.body != null && !js.isUndefined(response.body) then
      response.body.cancel()

  private def contentLength(response: js.Dynamic): Option[Long] =
    val value = response.headers.get("Content-Length").asInstanceOf[String | Null]
    if value == null then None else value.toLongOption

  private def rangeHeader(range: ByteRange): String =
    s"bytes=${range.offset}-${range.offset + range.length.toLong - 1L}"

  private def matchesContentRange(value: String, start: Long, end: Long): Boolean =
    val prefix = s"bytes $start-$end/"
    value.startsWith(prefix) && value.length > prefix.length

  private def statusError(key: StoreKey, status: Int): StoreError = status match
    case 401 => StoreError.Unauthorized(key)
    case 403 => StoreError.Forbidden(key)
    case 404 => StoreError.NotFound(key)
    case found => StoreError.Transport(key, s"HTTP status $found", transient = found >= 500)

object FetchStore:
  def apply(baseUrl: String)(using ExecutionContext): Either[String, FetchStore] =
    if !baseUrl.startsWith("http://") && !baseUrl.startsWith("https://") then
      Left("Fetch store base URL must use http or https")
    else
      val normalized = if baseUrl.endsWith("/") then baseUrl else s"$baseUrl/"
      Right(new FetchStore(normalized, mutable.ArrayBuffer.empty))
