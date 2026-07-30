package zarr4s

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.nio.file.Files

class JvmStoreSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def key(value: String): StoreKey = zvalue(StoreKey.from(value))
  private def range(offset: Long, length: Long): ByteRange = zvalue(ByteRange(offset, length))
  private def count(value: Long): ByteCount = zvalue(ByteCount(value))

  test("filesystem store confines keys and performs positional reads"):
    val root = Files.createTempDirectory("zarr4s-core-file-store")
    val nested = Files.createDirectories(root.resolve("c/0"))
    Files.write(nested.resolve("1"), Array[Byte](0, 1, 2, 3, 4, 5))
    val store = JvmFileStore.open(root).fold(fail(_), identity)
    assertEquals(store.length(key("c/0/1")), Right(6L))
    assertEquals(
      store.read(key("c/0/1"), range(2L, 3L)).map(_.toArray.toVector),
      Right(Vector[Byte](2, 3, 4))
    )
    assert(store.readAll(key("c/0/1"), count(5L)).isLeft)
    assert(store.readAll(key("missing"), count(10L)).isLeft)

  test("filesystem store rejects symlinks that escape its root"):
    val root = Files.createTempDirectory("zarr4s-core-confined")
    val outside = Files.createTempFile("zarr4s-core-outside", ".bin")
    Files.write(outside, Array[Byte](9, 8, 7))
    val link = root.resolve("escape")
    try
      Files.createSymbolicLink(link, outside)
      val store = JvmFileStore.open(root).fold(fail(_), identity)
      assert(store.readAll(key("escape"), count(10L)).isLeft)
    catch case _: UnsupportedOperationException => ()

  test("HTTP store validates range status, Content-Range, and lengths"):
    val payload = Array.tabulate[Byte](32)(_.toByte)
    withServer(payload, ignoreRange = false): base =>
      val store = JvmHttpStore(base).fold(fail(_), identity)
      assertEquals(store.length(key("object")), Right(32L))
      assertEquals(
        store.read(key("object"), range(5L, 7L)).map(_.toArray.toVector),
        Right(payload.slice(5, 12).toVector)
      )
      assertEquals(store.readAll(key("object"), count(32L)).map(_.length), Right(32))
      assert(store.readAll(key("object"), count(16L)).isLeft)

  test("HTTP store rejects a server that ignores Range without consuming it as data"):
    val payload = Array.tabulate[Byte](32)(_.toByte)
    withServer(payload, ignoreRange = true): base =>
      val store = JvmHttpStore(base).fold(fail(_), identity)
      assert(store.read(key("object"), range(5L, 7L)) match
        case Left(StoreError.RangeIgnored(_, _)) => true
        case _                                   => false)

  private def withServer(payload: Array[Byte], ignoreRange: Boolean)(body: URI => Unit): Unit =
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/store/object",
      new HttpHandler:
        def handle(exchange: HttpExchange): Unit =
          try
            exchange.getResponseHeaders.add("Content-Length", payload.length.toString)
            if exchange.getRequestMethod == "HEAD" then exchange.sendResponseHeaders(200, -1L)
            else
              val rangeHeader = exchange.getRequestHeaders.getFirst("Range")
              if rangeHeader != null && !ignoreRange then
                val bounds = rangeHeader.stripPrefix("bytes=").split("-").map(_.toInt)
                val selected = payload.slice(bounds(0), bounds(1) + 1)
                exchange.getResponseHeaders.set(
                  "Content-Range",
                  s"bytes ${bounds(0)}-${bounds(1)}/${payload.length}"
                )
                exchange.sendResponseHeaders(206, selected.length.toLong)
                exchange.getResponseBody.write(selected)
              else
                exchange.sendResponseHeaders(200, payload.length.toLong)
                exchange.getResponseBody.write(payload)
          finally exchange.close()
    )
    server.start()
    try
      body(URI.create(s"http://127.0.0.1:${server.getAddress.getPort}/store/"))
    finally server.stop(0)
