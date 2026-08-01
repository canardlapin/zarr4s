package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global

class BrowserZlibSuite extends munit.FunSuite:
  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("browser decodes a Python zlib chunk when the deflate stream capability is present"):
    if BrowserZlib.available then
      BrowserZlib
        .decode(ZarrBinaryFixtures.directZlibChunk, count(12))
        .map: result =>
          assertEquals(result, Right(ZarrBinaryFixtures.directDecodedChunk))
    else
      BrowserZlib
        .decode(ZarrBinaryFixtures.directZlibChunk, count(12))
        .map: result =>
          assert(result match
            case Left(CodecError.UnsupportedCapability("zlib", "browser")) => true
            case _                                                         => false)

  test("browser zlib enforces decoded limits before invoking the platform"):
    BrowserZlib
      .decode(
        OwnedBytes.empty,
        count(10),
        DecodeLimits(count(5))
      )
      .map: result =>
        assert(result match
          case Left(CodecError.DecodedLimitExceeded(5L, 10L)) => true
          case _                                              => false)
