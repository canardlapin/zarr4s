package zarr4s

class V2ReaderSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  private def full(opened: OpenedArray): ReadResult =
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    zvalue(opened.readRegion(region))

  test("sync reader opens a standalone v2 C-order array"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayC),
          "0.0" -> HierarchyFixtures.int16LittleChunk
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    assertEquals(opened.format, ZarrFormat.V2)
    assertEquals(shorts(full(opened)), Vector[Short](1, 2, 3, 4, 5, 6))

  test("sync reader lowers v2 F-order and big endian through the codec program"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayF),
          "0/0" -> HierarchyFixtures.int16BigFortranChunk
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    assertEquals(shorts(full(opened)), Vector[Short](1, 2, 3, 4, 5, 6))

  test("sync reader opens a v2 shuffle-filtered array"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayShuffle),
          "0.0" -> ZarrBinaryFixtures.directShuffledChunk
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    assertEquals(shorts(full(opened)), Vector[Short](1, -2, 300, 4, 5, -6))

  test("sync reader opens a Zarr-Python v2 delta plus shuffle array"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayDelta),
          "0.0" -> ZarrBinaryFixtures.v2DeltaShuffledChunk
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    assertEquals(shorts(full(opened)), Vector[Short](100, 102, 98, 99, 101, 98))

  test("v2 consolidated hierarchy discovers and opens children without child metadata reads"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zmetadata" -> HierarchyFixtures.bytes(HierarchyFixtures.v2Consolidated),
          "bold/0.0" -> HierarchyFixtures.int16LittleChunk
        )
      )
    )
    val root = zvalue(SyncZarr.openGroup(store, consolidation = ConsolidationMode.Require))
    assertEquals(root.format, ZarrFormat.V2)
    assertEquals(zvalue(root.children).map(_.path.value), Vector("bold", "derived"))
    store.clearTrace()
    val opened = zvalue(root.openArray("bold"))
    assertEquals(store.trace, Vector.empty)
    assertEquals(shorts(full(opened)), Vector[Short](1, 2, 3, 4, 5, 6))

  test("explicit v3 navigation works without consolidation while discovery refuses"):
    val store = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Group),
          "bold/zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Array),
          "bold/c/0/0" -> HierarchyFixtures.int16LittleChunk
        )
      )
    )
    val root = zvalue(SyncZarr.openGroup(store))
    assert(root.children.isLeft)
    val opened = zvalue(root.openArray("bold"))
    assertEquals(opened.format, ZarrFormat.V3)
    assertEquals(shorts(full(opened)), Vector[Short](1, 2, 3, 4, 5, 6))

  test("v3 consolidation policy is explicit and strict"):
    val malformed = HierarchyFixtures.v3Consolidated.replace(
      "\"metadata\":{",
      "\"metadata\":[] ,\"ignored\":{"
    )
    val store = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> HierarchyFixtures.bytes(malformed)
        )
      )
    )
    assert(SyncZarr.openGroup(store, consolidation = ConsolidationMode.Prefer).isLeft)
    assert(SyncZarr.openGroup(store, consolidation = ConsolidationMode.Ignore).isRight)

    val plain = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> HierarchyFixtures.bytes(HierarchyFixtures.v3Group)
        )
      )
    )
    assert(SyncZarr.openGroup(plain, consolidation = ConsolidationMode.Require).isLeft)
