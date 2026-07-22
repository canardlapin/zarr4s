package scalafim.zarr

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BrowserGzipSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("browser gzip round-trips when the stream capability is present"):
    if BrowserGzip.available then
      val decoded = OwnedBytes.copyOf(Array.tabulate[Byte](4096)(index => (index % 251).toByte))
      BrowserGzip.encode(decoded).flatMap:
        case Left(error) => fail(error.message)
        case Right(encoded) => BrowserGzip.decode(encoded, count(decoded.length)).map: result =>
          assertEquals(result, Right(decoded))
    else
      BrowserGzip.decode(OwnedBytes.empty, count(0)).map: result =>
        assert(result match
          case Left(CodecError.UnsupportedCapability("gzip", "browser")) => true
          case _ => false
        )

  test("browser gzip enforces decoded limits before invoking the platform"):
    BrowserGzip.decode(
      OwnedBytes.empty,
      count(10),
      DecodeLimits(count(5))
    ).map: result =>
      assert(result match
        case Left(CodecError.DecodedLimitExceeded(5L, 10L)) => true
        case _ => false
      )

  test("browser decodes the Zarr-Python 3.2.1 gzip chunk"):
    if BrowserGzip.available then
      BrowserGzip.decode(ZarrBinaryFixtures.directGzipChunk, count(12)).map: result =>
        assertEquals(result, Right(ZarrBinaryFixtures.directDecodedChunk))
    else
      BrowserGzip.decode(ZarrBinaryFixtures.directGzipChunk, count(12)).map: result =>
        assert(result.isLeft)

  test("browser gzip writes through the asynchronous Zarr interpreter"):
    val descriptor = ZarrMetadata.parse(ZarrBinaryFixtures.directGzipMetadata) match
      case Right(ZarrNodeMetadata.Array(metadata)) => zvalue(ArrayDescriptor.compile(metadata))
      case Right(_) => fail("expected array metadata")
      case Left(error) => fail(error.message)
    val block = PrimitiveBlock.Int16(OwnedShorts.copyOf(
      Array[Short](1, -2, 300, 4, 5, -6)
    ))
    val provider = AsyncChunkProvider.fromSync(new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] = Right(ChunkPayload.Values(block))
    )
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter.create(
      store,
      descriptor,
      provider,
      runtime = BrowserCodecRuntime.portable
    ).flatMap:
      case WriteOutcome.Incomplete(_, error) if !BrowserGzip.available =>
        Future.successful(assert(error.message.contains("gzip is unavailable")))
      case WriteOutcome.Incomplete(_, error) => fail(error.message)
      case WriteOutcome.Complete(_) => BrowserZarr.openArray(
        store,
        runtime = BrowserCodecRuntime.portable
      ).flatMap:
        case Left(error) => fail(error.message)
        case Right(opened) =>
          val region = zvalue(Region.within(
            descriptor.shape,
            zvalue(Coordinate(0L, 0L)),
            descriptor.shape
          ))
          opened.readRegion(region).map:
            case Left(error) => fail(error.message)
            case Right(result) => result.block match
              case PrimitiveBlock.Int16(values) =>
                assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
              case _ => fail("expected int16")
