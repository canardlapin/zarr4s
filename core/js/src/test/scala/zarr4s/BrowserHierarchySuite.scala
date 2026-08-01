package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BrowserHierarchySuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  private def full(opened: BrowserOpenedArray) =
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    opened.readRegion(region)

  test("browser reader opens and decodes a v2 F-order array"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayF),
          "0/0" -> HierarchyFixtures.int16BigFortranChunk
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          full(opened).map:
            case Left(error)   => fail(error.message)
            case Right(result) =>
              assertEquals(opened.format, ZarrFormat.V2)
              assertEquals(shorts(result), Vector[Short](1, 2, 3, 4, 5, 6))

  test("browser v2 lowering reuses the portable gzip executor"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayGzip),
          "0.0" -> ZarrBinaryFixtures.directGzipChunk
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          full(opened).map:
            case Left(error)   => fail(error.message)
            case Right(result) =>
              assertEquals(shorts(result), Vector[Short](1, -2, 300, 4, 5, -6))

  test("browser opens a common v2 zlib array"):
    if BrowserZlib.available then
      val store = zvalue(
        AsyncMemoryStore(
          Map(
            ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayZlib),
            "0.0" -> ZarrBinaryFixtures.directZlibChunk
          )
        )
      )
      BrowserZarr
        .openArray(store)
        .flatMap:
          case Left(error)   => fail(error.message)
          case Right(opened) =>
            full(opened).map:
              case Left(error)   => fail(error.message)
              case Right(result) =>
                assertEquals(shorts(result), Vector[Short](1, -2, 300, 4, 5, -6))
    else Future.successful(())

  test("browser hierarchy uses v3 inline consolidation without child metadata reads"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Consolidated),
          "bold/c/0/0" -> HierarchyFixtures.int16LittleChunk
        )
      )
    )
    BrowserZarr
      .openGroup(store, consolidation = ConsolidationMode.Require)
      .flatMap:
        case Left(error) => fail(error.message)
        case Right(root) =>
          assertEquals(zvalue(root.children).map(_.path.value), Vector("bold", "derived"))
          store.clearTrace()
          root
            .openArray("bold")
            .flatMap:
              case Left(error)   => fail(error.message)
              case Right(opened) =>
                assertEquals(store.trace, Vector.empty)
                full(opened).map:
                  case Left(error)   => fail(error.message)
                  case Right(result) =>
                    assertEquals(opened.format, ZarrFormat.V3)
                    assertEquals(shorts(result), Vector[Short](1, 2, 3, 4, 5, 6))

  test("browser explicit navigation remains lawful without consolidation"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Group),
          "bold/zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Array)
        )
      )
    )
    BrowserZarr
      .openGroup(store)
      .flatMap:
        case Left(error) => fail(error.message)
        case Right(root) =>
          assert(root.children.isLeft)
          root
            .openArray("bold")
            .map:
              case Left(error)   => fail(error.message)
              case Right(opened) => assertEquals(opened.path.value, "bold")
