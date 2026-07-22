package scalafim.zarr

class ZarrJavaInteropSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("portable reader opens the pinned zarr-java 0.1.3 fixture"):
    val metadataBytes = OwnedBytes.copyOf(ZarrJavaFixtures.metadata.iterator.map(_.toByte).toArray)
    assertEquals(
      PortableSha256.digest(metadataBytes).value,
      ZarrJavaFixtures.metadataSha256
    )
    assertEquals(
      PortableSha256.digest(ZarrJavaFixtures.chunk).value,
      ZarrJavaFixtures.chunkSha256
    )

    val store = zvalue(MemoryStore(ZarrJavaFixtures.objects))
    val opened = zvalue(SyncZarr.openArray(store))
    assertEquals(opened.format, ZarrFormat.V3)
    assertEquals(opened.descriptor.shape.toVector, Vector(2L, 3L))
    assertEquals(
      opened.descriptor.dimensionNames,
      Some(Vector(Some("y"), Some("x")))
    )
    val region = zvalue(Region.within(
      opened.descriptor.shape,
      zvalue(Coordinate(0L, 0L)),
      opened.descriptor.shape
    ))
    val result = zvalue(opened.readRegion(region))
    val values = result.block match
      case PrimitiveBlock.Int16(found) => found.toArray.toVector
      case _ => fail("expected int16 result")
    assertEquals(values, Vector[Short](1, -2, 300, 4, 5, -6))
    assertEquals(result.receipt.objectRequests, 1)
