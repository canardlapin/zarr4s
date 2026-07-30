package zarr4s

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class JvmHttpReaderSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def count(value: Long): ByteCount = zvalue(ByteCount(value))

  private def namespace(value: String): CacheNamespace =
    zvalue(CacheNamespace.from(value))

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(found) => found.toArray.toVector
    case _                           => fail("expected int16 result")

  test("JVM HTTP reader emits the same two-range shard trace as BrowserZarr"):
    val objects = Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
    )
    withServer(objects): (base, ranges) =>
      val store = JvmHttpStore(base).fold(fail(_), identity)
      val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
      clear(ranges)
      val region = zvalue(
        Region.within(
          opened.descriptor.shape,
          zvalue(Coordinate(0L, 0L)),
          opened.descriptor.shape
        )
      )
      val result = zvalue(opened.readRegion(region))
      assertEquals(result.receipt.rangeRequests, 2)
      assertEquals(snapshot(ranges), Vector("bytes=0-67", "bytes=68-83"))

  test("portable AsyncZarr over blocking JVM HTTP reuses shard ranges from a revision cache"):
    val objects = Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
    )
    withServer(objects): (base, ranges) =>
      val blocking = JvmHttpStore(base).fold(fail(_), identity)
      val executor = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
      try
        given ExecutionContext = executor
        val async = BlockingObjectReaderAdapter(blocking, executor)
        val cache = ObjectReadCache(
          namespace("jvm-http-fixture-v1"),
          CacheLimits(16, count(4096L))
        )
        val reader = CachingAsyncObjectReader(async, cache)
        val result = AsyncZarr
          .openArray(reader)
          .flatMap:
            case Left(error)   => fail(error.message)
            case Right(opened) =>
              val region = zvalue(
                Region.within(
                  opened.descriptor.shape,
                  zvalue(Coordinate(0L, 0L)),
                  opened.descriptor.shape
                )
              )
              clear(ranges)
              opened
                .readRegion(region)
                .flatMap:
                  case Left(error)  => fail(error.message)
                  case Right(first) =>
                    opened
                      .readRegion(region)
                      .map:
                        case Left(error)   => fail(error.message)
                        case Right(second) =>
                          assertEquals(shorts(second), shorts(first))
                          assertEquals(snapshot(ranges), Vector("bytes=0-67", "bytes=68-83"))
                          assertEquals(cache.stats.downstreamRequests, 3L)
        Await.result(result, Duration("10s"))
      finally executor.shutdown()

  private def withServer(objects: Map[String, OwnedBytes])(
      body: (URI, mutable.ArrayBuffer[String]) => Unit
  ): Unit =
    val ranges = mutable.ArrayBuffer.empty[String]
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/store/",
      new HttpHandler:
        def handle(exchange: HttpExchange): Unit =
          try
            val name = exchange.getRequestURI.getPath.stripPrefix("/store/")
            objects.get(name) match
              case None        => exchange.sendResponseHeaders(404, -1L)
              case Some(owned) =>
                val all = owned.toArray
                if exchange.getRequestMethod == "HEAD" then
                  exchange.getResponseHeaders.set("Content-Length", all.length.toString)
                  exchange.sendResponseHeaders(200, -1L)
                else
                  val range = exchange.getRequestHeaders.getFirst("Range")
                  if range == null then
                    exchange.getResponseHeaders.set("Content-Length", all.length.toString)
                    exchange.sendResponseHeaders(200, all.length.toLong)
                    exchange.getResponseBody.write(all)
                  else
                    ranges.synchronized:
                      ranges += range
                    val bounds = range.stripPrefix("bytes=").split("-").map(_.toInt)
                    val selected = all.slice(bounds(0), bounds(1) + 1)
                    exchange.getResponseHeaders.set(
                      "Content-Range",
                      s"bytes ${bounds(0)}-${bounds(1)}/${all.length}"
                    )
                    exchange.sendResponseHeaders(206, selected.length.toLong)
                    exchange.getResponseBody.write(selected)
          finally exchange.close()
    )
    server.start()
    try
      val base = URI.create(s"http://127.0.0.1:${server.getAddress.getPort}/store/")
      body(base, ranges)
    finally server.stop(0)

  private def snapshot(ranges: mutable.ArrayBuffer[String]): Vector[String] =
    ranges.synchronized:
      ranges.toVector

  private def clear(ranges: mutable.ArrayBuffer[String]): Unit =
    ranges.synchronized:
      ranges.clear()
