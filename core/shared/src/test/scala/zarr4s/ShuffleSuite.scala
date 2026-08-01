package zarr4s

class ShuffleSuite extends munit.FunSuite:
  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("shuffle matches the numcodecs byte-plane layout and reverses it"):
    val encoded = Shuffle.encode(ZarrBinaryFixtures.directDecodedChunk, 2) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(encoded, ZarrBinaryFixtures.directShuffledChunk)
    assertEquals(
      Shuffle.decode(encoded, count(12), DecodeLimits.default, 2),
      Right(ZarrBinaryFixtures.directDecodedChunk)
    )

  test("shuffle rejects non-integral payloads and decoded limits"):
    assert(Shuffle.encode(OwnedBytes.copyOf(Array[Byte](1, 2, 3)), 2).isLeft)
    assert(
      Shuffle
        .decode(
          ZarrBinaryFixtures.directShuffledChunk,
          count(12),
          DecodeLimits(count(8)),
          2
        )
        .isLeft
    )
