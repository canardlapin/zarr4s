package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BrowserZlibSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

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

  test("browser zlib bounded decode rejects an expanded stream over its limit"):
    if BrowserZlib.available then
      val decoded = OwnedBytes.copyOf(Array.fill[Byte](4096)(7.toByte))
      BrowserZlib
        .encode(decoded)
        .flatMap:
          case Left(error)    => fail(error.message)
          case Right(encoded) =>
            BrowserZlib
              .decodeBounded(ZlibCodec(1), encoded, DecodeLimits(count(128L)))
              .map: result =>
                assertEquals(
                  result,
                  Left(CodecError.DecodedLimitExceeded(128L, decoded.length.toLong))
                )
    else Future.successful(())

  test("browser zlib writes and reads an outer-codec sharded array"):
    val descriptor = ZarrMetadata.parse(ZarrBinaryFixtures.outerZlibShardedMetadata) match
      case Right(ZarrNodeMetadata.Array(metadata)) => zvalue(ArrayDescriptor.compile(metadata))
      case Right(_)                                => fail("expected array metadata")
      case Left(error)                             => fail(error.message)
    val provider = AsyncChunkProvider.fromSync(
      new ChunkProvider:
        def chunk(
            coordinate: ChunkCoordinate,
            storedShape: Shape
        ): Either[ZarrError, ChunkPayload] = coordinate.toVector match
          case Vector(0L, 0L) =>
            Right(
              ChunkPayload.Values(
                PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, 2, 3, 4)))
              )
            )
          case Vector(1L, 1L) =>
            Right(
              ChunkPayload.Values(
                PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](13, 14, 15, 16)))
              )
            )
          case _ => Right(ChunkPayload.Fill)
    )
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(
        store,
        descriptor,
        provider,
        runtime = BrowserCodecRuntime.portable
      )
      .flatMap:
        case WriteOutcome.Incomplete(_, error) if !BrowserZlib.available =>
          Future.successful(assert(error.message.contains("zlib")))
        case WriteOutcome.Incomplete(_, error) => fail(error.message)
        case WriteOutcome.Complete(receipt)    =>
          assertEquals(receipt.encodedChunks, 2L)
          BrowserZarr
            .openArray(store, runtime = BrowserCodecRuntime.portable)
            .flatMap:
              case Left(error)   => fail(error.message)
              case Right(opened) =>
                val region = zvalue(
                  Region.within(
                    descriptor.shape,
                    zvalue(Coordinate(1L, 1L)),
                    zvalue(Shape(3L, 3L))
                  )
                )
                opened
                  .readRegion(region)
                  .map:
                    case Left(error)   => fail(error.message)
                    case Right(result) =>
                      result.block match
                        case PrimitiveBlock.Int16(values) =>
                          assertEquals(
                            values.toArray.toVector,
                            Vector[Short](4, 0, 0, 0, 13, 14, 0, 15, 16)
                          )
                        case _ => fail("expected int16 result")
                      assertEquals(result.receipt.objectRequests, 1)
                      assertEquals(result.receipt.rangeRequests, 0)
                      assertEquals(result.receipt.lengthRequests, 0)
