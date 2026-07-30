package zarr4s.external

import scala.concurrent.ExecutionContext.Implicits.global
import zarr4s.*

class ExternalCodecJsSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  private def completed(outcome: WriteOutcome): WriteReceipt = outcome match
    case WriteOutcome.Complete(receipt)    => receipt
    case WriteOutcome.Incomplete(_, error) => fail(error.message)

  private def readAll(objects: Map[String, OwnedBytes]) =
    val store = zvalue(AsyncMemoryStore(objects))
    BrowserZarr
      .openArray(
        store,
        capabilities = ExternalXorFixture.capabilities,
        runtime = ExternalXorFixture.asyncRuntime
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
              case Right(result) => result

  test("external provider reads a direct array on Scala.js"):
    readAll(ExternalXorFixture.directObjects).map: result =>
      assertEquals(shorts(result), Vector[Short](1, 2, 3, 4, 5, 6))

  test("external provider reads an indexed shard on Scala.js"):
    readAll(ExternalXorFixture.shardedObjects).map: result =>
      assertEquals(shorts(result), ExternalXorFixture.fullValues)

  test("external provider writes a direct array through the async core"):
    val descriptor = zvalue(ExternalXorFixture.descriptor(ExternalXorFixture.directMetadata))
    val provider = AsyncChunkProvider.fromSync(
      new ChunkProvider:
        def chunk(
            coordinate: ChunkCoordinate,
            storedShape: Shape
        ): Either[ZarrError, ChunkPayload] = Right(
          ChunkPayload.Values(
            PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, 2, 3, 4, 5, 6)))
          )
        )
    )
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(
        store,
        descriptor,
        provider,
        runtime = ExternalXorFixture.asyncRuntime
      )
      .flatMap: outcome =>
        completed(outcome)
        assertEquals(store.snapshot("c/0/0"), ExternalXorFixture.directChunk)
        readAll(store.snapshot).map: result =>
          assertEquals(shorts(result), Vector[Short](1, 2, 3, 4, 5, 6))

  test("external provider writes an indexed shard through the async core"):
    val descriptor = zvalue(ExternalXorFixture.descriptor(ExternalXorFixture.shardedMetadata))
    val provider = AsyncChunkProvider.fromSync(
      new ChunkProvider:
        def chunk(
            coordinate: ChunkCoordinate,
            storedShape: Shape
        ): Either[ZarrError, ChunkPayload] =
          val index = (coordinate.axis(0) * 2L + coordinate.axis(1)).toInt
          Right(ChunkPayload.Values(ExternalXorFixture.shardedChunks(index)))
    )
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(
        store,
        descriptor,
        provider,
        runtime = ExternalXorFixture.asyncRuntime
      )
      .flatMap: outcome =>
        completed(outcome)
        assertEquals(store.snapshot("c/0/0"), ExternalXorFixture.shardedObject)
        readAll(store.snapshot).map: result =>
          assertEquals(shorts(result), ExternalXorFixture.fullValues)
