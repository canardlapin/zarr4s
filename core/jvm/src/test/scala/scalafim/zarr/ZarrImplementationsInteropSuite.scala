package scalafim.zarr

class ZarrImplementationsInteropSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("JVM reads the pinned zarr_implementations v2 gzip corpus chunk"):
    assertEquals(
      PortableSha256.digest(ZarrImplementationsFixtures.gzipChunk000).value,
      ZarrImplementationsFixtures.chunkSha256
    )
    val store = zvalue(MemoryStore(ZarrImplementationsFixtures.objects))
    val root = zvalue(SyncZarr.openGroup(store, runtime = JvmCodecRuntime.portable))
    assertEquals(root.format, ZarrFormat.V2)
    val opened = zvalue(root.openArray("gzip"))
    assertEquals(opened.format, ZarrFormat.V2)
    assertEquals(opened.descriptor.shape.toVector, Vector(512L, 512L, 3L))
    val region = zvalue(Region.within(
      opened.descriptor.shape,
      zvalue(Coordinate(0L, 0L, 0L)),
      zvalue(Shape(4L, 4L, 1L))
    ))
    val result = zvalue(opened.readRegion(region))
    val values = result.block match
      case PrimitiveBlock.UInt8(found) =>
        found.toArray.iterator.map(value => value & 0xff).toVector
      case _ => fail("expected uint8 result")
    assertEquals(values, ZarrImplementationsFixtures.expectedCorner)
    assertEquals(result.receipt.objectRequests, 1)
