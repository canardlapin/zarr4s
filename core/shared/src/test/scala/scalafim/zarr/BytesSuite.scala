package scalafim.zarr

class BytesSuite extends munit.FunSuite:
  private def byteCount(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("owned bytes copy at the public boundary"):
    val source = Array[Byte](1, 2, 3)
    val owned = OwnedBytes.copyOf(source)
    source(0) = 9
    assertEquals(owned.toArray.toVector, Vector[Byte](1, 2, 3))
    val exported = owned.toArray
    exported(1) = 9
    assertEquals(owned.toArray.toVector, Vector[Byte](1, 2, 3))

  test("CRC32C matches the standard check vector"):
    val bytes = OwnedBytes.copyOf("123456789".getBytes("UTF-8"))
    assertEquals(Crc32c.checksum(bytes), 0xe3069283L)
    assertEquals(Crc32c.verifyAndStrip(Crc32c.append(bytes)), Right(bytes))

  test("CRC32C detects payload and checksum corruption"):
    val encoded = Crc32c.append(OwnedBytes.copyOf(Array[Byte](1, 2, 3, 4))).toArray
    encoded(1) = (encoded(1) ^ 0x01).toByte
    assert(Crc32c.verifyAndStrip(OwnedBytes.copyOf(encoded)).isLeft)

  test("decoded length enforces both expectation and resource limit"):
    val bytes = OwnedBytes.copyOf(Array.fill[Byte](8)(1))
    assertEquals(
      DecodedLength.validate(bytes, byteCount(8), DecodeLimits(byteCount(8))),
      Right(bytes)
    )
    assert(DecodedLength.validate(bytes, byteCount(7), DecodeLimits(byteCount(8))).isLeft)
    assert(DecodedLength.validate(bytes, byteCount(8), DecodeLimits(byteCount(4))).isLeft)

  test("byte ranges reject overflow"):
    assert(ByteRange(Long.MaxValue, 1L).isLeft)
    assertEquals(ByteRange(5L, 3L).flatMap(_.endExclusive), Right(8L))
