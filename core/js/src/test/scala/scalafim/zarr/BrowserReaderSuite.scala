package scalafim.zarr

import scala.concurrent.ExecutionContext.Implicits.global

class BrowserReaderSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _ => fail("expected int16 result")

  test("BrowserZarr opens and decodes a Zarr-Python direct gzip array"):
    val store = zvalue(AsyncMemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.directGzipMetadata),
      "c/0/0" -> ZarrBinaryFixtures.directGzipChunk
    )))
    BrowserZarr.openArray(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(opened) =>
        val region = zvalue(Region.within(
          opened.descriptor.shape,
          zvalue(Coordinate(0L, 0L)),
          opened.descriptor.shape
        ))
        store.clearTrace()
        opened.readRegion(region).map:
          case Left(error) => fail(error.message)
          case Right(result) =>
            assertEquals(shorts(result), Vector[Short](1, -2, 300, 4, 5, -6))
            assertEquals(result.receipt.objectRequests, 1)
            assertEquals(store.trace.length, 1)

  test("BrowserZarr executes the same two-phase start-indexed shard plan"):
    val store = zvalue(AsyncMemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
    )))
    BrowserZarr.openArray(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(opened) =>
        val region = zvalue(Region.within(
          opened.descriptor.shape,
          zvalue(Coordinate(0L, 0L)),
          opened.descriptor.shape
        ))
        store.clearTrace()
        opened.readRegion(region).map:
          case Left(error) => fail(error.message)
          case Right(result) =>
            assertEquals(shorts(result), Vector[Short](
              1, 2, 0, 0,
              3, 4, 0, 0,
              0, 0, 13, 14,
              0, 0, 15, 16
            ))
            assertEquals(result.receipt.rangeRequests, 2)
            assertEquals(store.trace.collect {
              case request @ ObjectRequest.Range(_, _) => request
            }.length, 2)

  test("BrowserZarr point reads preserve order and duplicates"):
    val store = zvalue(AsyncMemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.directGzipMetadata),
      "c/0/0" -> ZarrBinaryFixtures.directGzipChunk
    )))
    BrowserZarr.openArray(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(opened) =>
        val points = zvalue(CoordinateBatch.within(opened.descriptor.shape, Seq(
          zvalue(Coordinate(1L, 2L)),
          zvalue(Coordinate(0L, 0L)),
          zvalue(Coordinate(1L, 2L))
        )))
        opened.readPoints(points).map:
          case Left(error) => fail(error.message)
          case Right(result) =>
            assertEquals(shorts(result), Vector[Short](-6, 1, -6))

  test("BrowserZarr materializes factored slices and duplicate gathers"):
    val store = zvalue(AsyncMemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.directGzipMetadata),
      "c/0/0" -> ZarrBinaryFixtures.directGzipChunk
    )))
    BrowserZarr.openArray(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(opened) =>
        val selection = zvalue(FactoredSelection.within(opened.descriptor.shape, Vector(
          AxisSelector.Indices(zvalue(AxisIndices.from(Vector(1L, 0L, 1L)))),
          AxisSelector.Slice(zvalue(AxisSlice(0L, 3L, 2L)))
        )))
        opened.read(selection).map:
          case Left(error) => fail(error.message)
          case Right(result) =>
            assertEquals(result.shape, zvalue(Shape(3L, 2L)))
            assertEquals(shorts(result), Vector[Short](4, -6, 1, 300, 4, -6))

  test("BrowserZarr executes end-indexed shards through an explicit length capability"):
    val store = zvalue(AsyncMemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedEndMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedEndObject
    )))
    BrowserZarr.openArray(store).flatMap:
      case Left(error) => fail(error.message)
      case Right(opened) =>
        val region = zvalue(Region.within(
          opened.descriptor.shape,
          zvalue(Coordinate(0L, 0L)),
          opened.descriptor.shape
        ))
        store.clearTrace()
        opened.readRegion(region).map:
          case Left(error) => fail(error.message)
          case Right(result) =>
            assertEquals(shorts(result), Vector[Short](
              1, 2, 0, 0,
              3, 4, 0, 0,
              0, 0, 13, 14,
              0, 0, 15, 16
            ))
            assertEquals(result.receipt.lengthRequests, 1)
            assertEquals(result.receipt.rangeRequests, 2)
