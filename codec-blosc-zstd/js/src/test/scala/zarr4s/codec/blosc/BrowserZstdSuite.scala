package zarr4s.codec.blosc

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import zarr4s.*

class BrowserZstdSuite extends munit.FunSuite:
  private val decoded = ZstdFixtures.directDecodedChunk
  private val decodedCount = ByteCount.unsafe(decoded.length.toLong)

  test("Scala.js provider round-trips zstd bytes with and without a checksum"):
    Future.sequence(List(false, true).map: checksum =>
      val codec = zcodec(3, checksum)
      BrowserZstd
        .encode(codec, decoded)
        .flatMap:
          case Left(error)    => fail(error.message)
          case Right(encoded) =>
            BrowserZstd
              .decode(codec, encoded, decodedCount, DecodeLimits.default)
              .map: result =>
                assertEquals(right(result), decoded))

  test("Scala.js provider decodes an independent zstd fixture"):
    BrowserZstd
      .decode(zcodec(3, false), ZstdFixtures.directZstdChunk, decodedCount, DecodeLimits.default)
      .map: result =>
        assertEquals(right(result), decoded)

  test("Scala.js provider rejects decoded limits before invoking zstd"):
    BrowserZstd
      .decode(
        zcodec(3, false),
        ZstdFixtures.directZstdChunk,
        decodedCount,
        DecodeLimits(ByteCount.unsafe(8L))
      )
      .map: result =>
        assertEquals(
          result,
          Left(CodecError.DecodedLimitExceeded(8L, decoded.length.toLong))
        )

  test("Scala.js provider reads a v2 zstd array"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          ".zarray" -> OwnedBytes.copyOf(ZstdFixtures.v2ArrayZstd.getBytes("UTF-8")),
          "0.0" -> ZstdFixtures.directZstdChunk
        )
      )
    )
    BrowserZarr
      .openArray(
        store,
        capabilities = BloscZstdProvider.capabilities(),
        runtime = BrowserBloscZstdRuntime.portable
      )
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
          opened
            .readRegion(region)
            .map:
              case Left(error)   => fail(error.message)
              case Right(result) =>
                result.block match
                  case PrimitiveBlock.Int16(values) =>
                    assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
                  case _ => fail("expected int16 result")

  test("optional runtime advertises zstd without changing the default browser runtime"):
    assert(!BrowserCodecRuntime.portable.executorNames.contains("zstd"))
    assert(BrowserBloscZstdRuntime.portable.executorNames.contains("zstd"))

  private def zcodec(level: Int, checksum: Boolean): ZstdCodec =
    ZstdCodec.create(level, checksum) match
      case Right(found) => found
      case Left(error)  => fail(error)

  private def right[A](value: Either[CodecError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def zvalue[A](value: Either[ZarrError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)
