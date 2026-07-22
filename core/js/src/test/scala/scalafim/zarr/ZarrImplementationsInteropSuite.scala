package scalafim.zarr

import scala.concurrent.ExecutionContext.Implicits.global

class ZarrImplementationsInteropSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("Scala.js reads the pinned zarr_implementations v2 gzip corpus chunk"):
    assertEquals(
      PortableSha256.digest(ZarrImplementationsFixtures.gzipChunk000).value,
      ZarrImplementationsFixtures.chunkSha256
    )
    val store = zvalue(AsyncMemoryStore(ZarrImplementationsFixtures.objects))
    BrowserZarr.openGroup(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(root) =>
        assertEquals(root.format, ZarrFormat.V2)
        root.openArray("gzip").flatMap:
          case Left(error) => fail(error.message)
          case Right(opened) =>
            assertEquals(opened.format, ZarrFormat.V2)
            assertEquals(opened.descriptor.shape.toVector, Vector(512L, 512L, 3L))
            val region = zvalue(Region.within(
              opened.descriptor.shape,
              zvalue(Coordinate(0L, 0L, 0L)),
              zvalue(Shape(4L, 4L, 1L))
            ))
            opened.readRegion(region).map:
              case Left(error) => fail(error.message)
              case Right(result) =>
                val values = result.block match
                  case PrimitiveBlock.UInt8(found) =>
                    found.toArray.iterator.map(value => value & 0xff).toVector
                  case _ => fail("expected uint8 result")
                assertEquals(values, ZarrImplementationsFixtures.expectedCorner)
                assertEquals(result.receipt.objectRequests, 1)
