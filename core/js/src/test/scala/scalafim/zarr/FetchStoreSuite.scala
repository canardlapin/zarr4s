package scalafim.zarr

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js
import scala.scalajs.js.typedarray.Uint8Array

class FetchStoreSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def key(value: String): StoreKey = zvalue(StoreKey.from(value))
  private def range(offset: Long, length: Long): ByteRange = zvalue(ByteRange(offset, length))
  private def count(value: Long): ByteCount = zvalue(ByteCount(value))
  private def namespace(value: String): CacheNamespace = zvalue(CacheNamespace.from(value))
  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(found) => found.toArray.toVector
    case _ => fail("expected int16 result")

  test("Fetch store validates 206 ranges, lengths, and whole-object limits"):
    val payload = OwnedBytes.copyOf(Array.tabulate[Byte](32)(_.toByte))
    withServer(Map("object" -> payload)): base =>
      val store = FetchStore(base).fold(fail(_), identity)
      for
        length <- store.length(key("object"))
        selected <- store.read(key("object"), range(5L, 7L))
        whole <- store.readAll(key("object"), count(32L))
        limited <- store.readAll(key("object"), count(16L))
      yield
        assertEquals(length, Right(32L))
        assertEquals(selected.map(_.toArray.toVector), Right(payload.toArray.slice(5, 12).toVector))
        assertEquals(whole.map(_.length), Right(32))
        assert(limited.isLeft)

  test("Fetch store rejects an ignored Range response"):
    val payload = OwnedBytes.copyOf(Array.tabulate[Byte](32)(_.toByte))
    withServer(Map("object" -> payload), ignoreRange = true): base =>
      val store = FetchStore(base).fold(fail(_), identity)
      store.read(key("object"), range(5L, 7L)).map: result =>
        assert(result match
          case Left(StoreError.RangeIgnored(_, _)) => true
          case _ => false
        )

  test("BrowserZarr over Fetch emits the same two-range shard trace"):
    val objects = Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
    )
    withServer(objects): base =>
      val store = FetchStore(base).fold(fail(_), identity)
      val cache = ObjectReadCache(
        namespace("fetch-fixture-v1"),
        CacheLimits(16, count(4096L))
      )
      val reader = CachingAsyncObjectReader(store, cache)
      BrowserZarr.openArray(reader).flatMap:
        case Left(error) => fail(error.message)
        case Right(opened) =>
          val region = zvalue(Region.within(
            opened.descriptor.shape,
            zvalue(Coordinate(0L, 0L)),
            opened.descriptor.shape
          ))
          store.clearTrace()
          opened.readRegion(region).flatMap:
            case Left(error) => fail(error.message)
            case Right(result) =>
              assertEquals(result.receipt.rangeRequests, 2)
              val firstTrace = store.trace
              assertEquals(firstTrace.collect {
                case request @ ObjectRequest.Range(_, _) => request
              }.length, 2)
              opened.readRegion(region).map:
                case Left(error) => fail(error.message)
                case Right(second) =>
                  assertEquals(shorts(second), shorts(result))
                  assertEquals(store.trace, firstTrace)
                  assertEquals(cache.stats.downstreamRequests, 3L)

  private def withServer[A](
      objects: Map[String, OwnedBytes],
      ignoreRange: Boolean = false
  )(body: String => Future[A]): Future[A] =
    val http = js.Dynamic.global.require("http")
    val handler: js.Function2[js.Dynamic, js.Dynamic, Unit] = (request, response) =>
      val path = request.url.asInstanceOf[String].stripPrefix("/store/")
      objects.get(path) match
        case None =>
          response.statusCode = 404
          response.end()
        case Some(payload) =>
          val all = payload.toArray
          val method = request.method.asInstanceOf[String]
          val rangeValue = request.headers.selectDynamic("range")
          if method == "HEAD" then
            response.statusCode = 200
            response.setHeader("Content-Length", all.length.toString)
            response.end()
          else if !js.isUndefined(rangeValue) && !ignoreRange then
            val bounds = rangeValue.asInstanceOf[String]
              .stripPrefix("bytes=")
              .split("-")
              .map(_.toInt)
            val selected = all.slice(bounds(0), bounds(1) + 1)
            response.statusCode = 206
            response.setHeader(
              "Content-Range",
              s"bytes ${bounds(0)}-${bounds(1)}/${all.length}"
            )
            response.setHeader("Content-Length", selected.length.toString)
            response.end(uint8(selected))
          else
            response.statusCode = 200
            response.setHeader("Content-Length", all.length.toString)
            response.end(uint8(all))
    val server = http.createServer(handler)
    val started = Promise[String]()
    server.listen(0, "127.0.0.1", () =>
      val port = server.address().port.asInstanceOf[Int]
      started.success(s"http://127.0.0.1:$port/store/")
    )
    started.future.flatMap(body).transformWith: result =>
      val closed = Promise[Unit]()
      server.close(() => closed.success(()))
      closed.future.transform(_ => result)

  private def uint8(bytes: Array[Byte]): Uint8Array =
    val result = new Uint8Array(bytes.length)
    var index = 0
    while index < bytes.length do
      result(index) = bytes(index)
      index += 1
    result
