package scalafim.zarr

class JvmGzipSuite extends munit.FunSuite:
  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("JVM gzip round-trips owned bytes with exact length"):
    val decoded = OwnedBytes.copyOf(Array.tabulate[Byte](4096)(index => (index % 251).toByte))
    val encoded = JvmGzip.encode(decoded) match
      case Right(found) => found
      case Left(error) => fail(error.message)
    assert(encoded.length < decoded.length)
    assertEquals(JvmGzip.decode(encoded, count(decoded.length)), Right(decoded))

  test("JVM gzip rejects wrong lengths, limits, and corruption"):
    val decoded = OwnedBytes.copyOf(Array.fill[Byte](1024)(7))
    val encoded = JvmGzip.encode(decoded).toOption.get
    assert(JvmGzip.decode(encoded, count(1023)).isLeft)
    assert(JvmGzip.decode(encoded, count(1024), DecodeLimits(count(100))).isLeft)
    val corrupt = encoded.toArray
    corrupt(corrupt.length / 2) = (corrupt(corrupt.length / 2) ^ 1).toByte
    assert(JvmGzip.decode(OwnedBytes.copyOf(corrupt), count(1024)).isLeft)

  test("JVM decodes the Zarr-Python 3.2.1 gzip chunk"):
    assertEquals(
      JvmGzip.decode(ZarrBinaryFixtures.directGzipChunk, count(12)),
      Right(ZarrBinaryFixtures.directDecodedChunk)
    )
