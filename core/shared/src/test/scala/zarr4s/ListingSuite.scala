package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global

class ListingSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes = HierarchyFixtures.bytes(value)

  private val v3Objects = Map(
    "zarr.json" -> bytes(HierarchyFixtures.v3Group),
    "bold/zarr.json" -> bytes(HierarchyFixtures.v3Array),
    "derived/zarr.json" -> bytes(HierarchyFixtures.v3Group),
    "derived/mask/zarr.json" -> bytes(HierarchyFixtures.v3Mask)
  )

  private val v2Objects = Map(
    ".zgroup" -> bytes(HierarchyFixtures.v2Group),
    ".zattrs" -> bytes("{\"title\":\"root\"}"),
    "bold/.zarray" -> bytes(HierarchyFixtures.v2ArrayC),
    "bold/.zattrs" -> bytes("{\"_ARRAY_DIMENSIONS\":[\"y\",\"x\"]}"),
    "derived/.zgroup" -> bytes(HierarchyFixtures.v2Group),
    "derived/.zattrs" -> bytes("{}"),
    "derived/mask/.zarray" -> bytes(HierarchyFixtures.v2ArrayC),
    "derived/mask/.zattrs" -> bytes("{}")
  )

  test("memory listing is sorted, recursive, and bounded"):
    val store = zvalue(
      MemoryStore(
        Map(
          "root/b" -> OwnedBytes.copyOf(Array[Byte](2)),
          "root/a" -> OwnedBytes.copyOf(Array[Byte](1)),
          "root/deep/c" -> OwnedBytes.copyOf(Array[Byte](3)),
          "other" -> OwnedBytes.copyOf(Array[Byte](4))
        )
      )
    )
    assertEquals(
      store.list(ZarrPath.root, 10).map(_.map(_.value)),
      Right(Vector("other", "root/a", "root/b", "root/deep/c"))
    )
    assertEquals(
      store.list(zvalue(ZarrPath("root")), 10).map(_.map(_.value)),
      Right(Vector("root/a", "root/b", "root/deep/c"))
    )
    assert(store.list(ZarrPath.root, 2).isLeft)

  test("sync v3 groups discover un-consolidated children through an explicit lister"):
    val store = zvalue(MemoryStore(v3Objects))
    val root = zvalue(SyncZarr.openGroup(store, lister = Some(store)))
    assertEquals(
      root.children.map(_.map(entry => entry.path.value -> entry.kind)),
      Right(Vector("bold" -> NodeKind.Array, "derived" -> NodeKind.Group))
    )
    val derived = zvalue(root.openGroup("derived"))
    assertEquals(derived.children.map(_.map(_.path.value)), Right(Vector("derived/mask")))

  test("sync v2 groups discover individual metadata and preserve attributes"):
    val store = zvalue(MemoryStore(v2Objects))
    val root = zvalue(SyncZarr.openGroup(store, lister = Some(store)))
    assertEquals(root.metadata.attributes.get("title"), Some(JsonValue.Str("root")))
    assertEquals(
      root.children.map(_.map(entry => entry.path.value -> entry.kind)),
      Right(Vector("bold" -> NodeKind.Array, "derived" -> NodeKind.Group))
    )
    val bold = zvalue(root.openArray("bold"))
    assertEquals(bold.format, ZarrFormat.V2)
    assertEquals(bold.descriptor.dimensionNames, Some(Vector(Some("y"), Some("x"))))

  test("discovery rejects entries, depth, and metadata reads over explicit limits"):
    val store = zvalue(MemoryStore(v3Objects))
    val entryLimited = zvalue(
      SyncZarr.openGroup(
        store,
        limits = OpenLimits(hierarchy = HierarchyLimits(maxDiscoveryEntries = 1)),
        lister = Some(store)
      )
    )
    assert(entryLimited.children.isLeft)

    val deepStore = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(HierarchyFixtures.v3Group),
          "a/b/c/zarr.json" -> bytes(HierarchyFixtures.v3Array)
        )
      )
    )
    val deepRoot = zvalue(
      SyncZarr.openGroup(
        deepStore,
        limits = OpenLimits(hierarchy = HierarchyLimits(maxDiscoveryDepth = 2)),
        lister = Some(deepStore)
      )
    )
    assert(deepRoot.children.isLeft)

    val readLimited = zvalue(
      SyncZarr.openGroup(
        store,
        limits = OpenLimits(hierarchy = HierarchyLimits(maxDiscoveryMetadataReads = 0)),
        lister = Some(store)
      )
    )
    assert(readLimited.children.isLeft)

  test("async groups expose non-blocking un-consolidated discovery"):
    val store = zvalue(AsyncMemoryStore(v3Objects))
    AsyncZarr
      .openGroup(store, lister = Some(store))
      .flatMap:
        case Left(error) => fail(error.message)
        case Right(root) =>
          assert(root.children.isLeft)
          root.discoverChildren.map:
            case Left(error)     => fail(error.message)
            case Right(children) =>
              assertEquals(
                children.map(entry => entry.path.value -> entry.kind),
                Vector("bold" -> NodeKind.Array, "derived" -> NodeKind.Group)
              )

  test("async v2 groups discover child metadata and attributes"):
    val store = zvalue(AsyncMemoryStore(v2Objects))
    AsyncZarr
      .openGroup(store, lister = Some(store))
      .flatMap:
        case Left(error) => fail(error.message)
        case Right(root) =>
          root.discoverChildren.flatMap:
            case Left(error)     => fail(error.message)
            case Right(children) =>
              assertEquals(children.map(_.path.value), Vector("bold", "derived"))
              root
                .openArray("bold")
                .map:
                  case Left(error)  => fail(error.message)
                  case Right(array) =>
                    assertEquals(array.format, ZarrFormat.V2)
                    assertEquals(
                      array.descriptor.dimensionNames,
                      Some(Vector(Some("y"), Some("x")))
                    )
