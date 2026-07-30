package zarr4s

class ZarrsInteropSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("JVM reads the independent zarrs end-indexed gzip shard corpus"):
    val store = zvalue(MemoryStore(ZarrsFixtures.objects))
    val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    val result = zvalue(opened.readRegion(region))
    val values = result.block match
      case PrimitiveBlock.UInt16(found) => found.toArray.toVector
      case _                            => fail("expected uint16 result")
    assertEquals(values, (0 until 64).map(_.toShort).toVector)
    assertEquals(result.receipt.touchedShards, 2)
    assertEquals(result.receipt.lengthRequests, 2)
