package zarr4s

import java.io.InputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import scala.util.control.NonFatal

final class JvmHttpStore private (
    baseUri: URI,
    client: HttpClient
) extends ObjectReader:
  def read(key: StoreKey, range: ByteRange): Either[StoreError, OwnedBytes] =
    if range.length.toLong == 0L then Right(OwnedBytes.empty)
    else
      val request = HttpRequest
        .newBuilder(uri(key))
        .header("Range", rangeHeader(range))
        .GET()
        .build()
      send(key, request).flatMap: response =>
        response.statusCode() match
          case 206 => validateRangeResponse(key, range, response)
          case 200 =>
            response.body().close()
            Left(StoreError.RangeIgnored(key, range))
          case status =>
            response.body().close()
            Left(statusError(key, status))

  def readAll(key: StoreKey, maxBytes: ByteCount): Either[StoreError, OwnedBytes] =
    val request = HttpRequest.newBuilder(uri(key)).GET().build()
    send(key, request).flatMap: response =>
      response.statusCode() match
        case status if status >= 200 && status < 300 =>
          val declared = response.headers().firstValueAsLong("Content-Length")
          if declared.isPresent && declared.getAsLong > maxBytes.toLong then
            response.body().close()
            Left(StoreError.ObjectTooLarge(key, maxBytes.toLong, declared.getAsLong))
          else readBounded(key, response.body(), maxBytes)
        case status =>
          response.body().close()
          Left(statusError(key, status))

  def length(key: StoreKey): Either[StoreError, Long] =
    val request = HttpRequest
      .newBuilder(uri(key))
      .method("HEAD", HttpRequest.BodyPublishers.noBody())
      .build()
    send(key, request).flatMap: response =>
      response.body().close()
      response.statusCode() match
        case status if status >= 200 && status < 300 =>
          val length = response.headers().firstValueAsLong("Content-Length")
          if length.isPresent then Right(length.getAsLong)
          else Left(StoreError.ObjectLengthUnavailable(key))
        case status => Left(statusError(key, status))

  private def validateRangeResponse(
      key: StoreKey,
      range: ByteRange,
      response: HttpResponse[InputStream]
  ): Either[StoreError, OwnedBytes] =
    val expectedEnd = range.offset + range.length.toLong - 1L
    val header = response.headers().firstValue("Content-Range")
    if header.isEmpty || !matchesContentRange(header.get(), range.offset, expectedEnd) then
      response.body().close()
      Left(StoreError.Transport(key, "invalid Content-Range response", transient = false))
    else readExact(key, response.body(), range.length)

  private def matchesContentRange(value: String, start: Long, end: Long): Boolean =
    val prefix = s"bytes $start-$end/"
    value.startsWith(prefix) && value.length > prefix.length

  private def readExact(
      key: StoreKey,
      input: InputStream,
      length: ByteCount
  ): Either[StoreError, OwnedBytes] =
    if length.toLong > Int.MaxValue.toLong then
      input.close()
      Left(StoreError.ObjectTooLarge(key, Int.MaxValue, length.toLong))
    else
      try
        val bytes = input.readNBytes(length.toLong.toInt)
        val extra = input.read()
        input.close()
        if bytes.length.toLong != length.toLong || extra >= 0 then
          Left(StoreError.Transport(key, "range body length mismatch", transient = false))
        else Right(OwnedBytes.unsafe(bytes))
      catch
        case NonFatal(error) =>
          input.close()
          Left(StoreError.Transport(key, error.getMessage, transient = true))

  private def readBounded(
      key: StoreKey,
      input: InputStream,
      limit: ByteCount
  ): Either[StoreError, OwnedBytes] =
    if limit.toLong > Int.MaxValue.toLong then
      input.close()
      Left(StoreError.ObjectTooLarge(key, Int.MaxValue, limit.toLong))
    else
      try
        val bytes = input.readNBytes(limit.toLong.toInt + 1)
        input.close()
        if bytes.length.toLong > limit.toLong then
          Left(StoreError.ObjectTooLarge(key, limit.toLong, bytes.length.toLong))
        else Right(OwnedBytes.unsafe(bytes))
      catch
        case NonFatal(error) =>
          input.close()
          Left(StoreError.Transport(key, error.getMessage, transient = true))

  private def send(
      key: StoreKey,
      request: HttpRequest
  ): Either[StoreError, HttpResponse[InputStream]] =
    try Right(client.send(request, HttpResponse.BodyHandlers.ofInputStream()))
    catch
      case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = true))

  private def uri(key: StoreKey): URI = baseUri.resolve(key.value)

  private def rangeHeader(range: ByteRange): String =
    s"bytes=${range.offset}-${range.offset + range.length.toLong - 1L}"

  private def statusError(key: StoreKey, status: Int): StoreError = status match
    case 401   => StoreError.Unauthorized(key)
    case 403   => StoreError.Forbidden(key)
    case 404   => StoreError.NotFound(key)
    case found => StoreError.Transport(key, s"HTTP status $found", transient = found >= 500)

object JvmHttpStore:
  def apply(
      baseUri: URI,
      client: HttpClient = HttpClient.newHttpClient()
  ): Either[String, JvmHttpStore] =
    if !baseUri.isAbsolute then Left("HTTP store base URI must be absolute")
    else if baseUri.getScheme != "http" && baseUri.getScheme != "https" then
      Left("HTTP store base URI must use http or https")
    else
      val value = baseUri.toString
      val normalized = URI.create(if value.endsWith("/") then value else s"$value/")
      Right(new JvmHttpStore(normalized, client))
