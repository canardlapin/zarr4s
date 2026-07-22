package scalafim.zarr

class HierarchySuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def v3Index(input: String = HierarchyFixtures.v3Consolidated): HierarchyIndex =
    ZarrMetadata.parse(input).flatMap:
      case ZarrNodeMetadata.Group(group) => HierarchyIndex.v3(
        ZarrPath.root,
        group,
        HierarchyLimits()
      ).flatMap(_.toRight(ZarrError.InvalidMetadata("$", "missing hierarchy")))
      case ZarrNodeMetadata.Array(_) => Left(ZarrError.UnsupportedNodeType("array"))
    match
      case Right(found) => found
      case Left(error) => fail(error.message)

  test("v3 inline consolidation indexes immediate and nested children"):
    val index = v3Index()
    assertEquals(index.entries.map(entry => entry.path.value -> entry.kind), Vector(
      "" -> NodeKind.Group,
      "bold" -> NodeKind.Array,
      "derived" -> NodeKind.Group,
      "derived/mask" -> NodeKind.Array
    ))
    assertEquals(
      index.children(ZarrPath.root).map(entry => entry.path.value -> entry.kind),
      Vector("bold" -> NodeKind.Array, "derived" -> NodeKind.Group)
    )
    assertEquals(
      index.children(zvalue(ZarrPath("derived"))).map(_.path.value),
      Vector("derived/mask")
    )

  test("unknown optional v3 consolidation is ignored but required unknown kinds refuse"):
    val optional = HierarchyFixtures.v3Consolidated
      .replace("\"kind\":\"inline\"", "\"kind\":\"future\"")
    ZarrMetadata.parse(optional) match
      case Right(ZarrNodeMetadata.Group(group)) =>
        assertEquals(zvalue(HierarchyIndex.v3(ZarrPath.root, group, HierarchyLimits())), None)
      case Right(_) => fail("expected group")
      case Left(error) => fail(error.message)

    val required = optional.replace("\"must_understand\":false", "\"must_understand\":true")
    ZarrMetadata.parse(required) match
      case Right(ZarrNodeMetadata.Group(group)) =>
        assert(HierarchyIndex.v3(ZarrPath.root, group, HierarchyLimits()).isLeft)
      case Right(_) => fail("expected group")
      case Left(error) => fail(error.message)

  test("v3 inline consolidation enforces entry and node limits"):
    ZarrMetadata.parse(HierarchyFixtures.v3Consolidated) match
      case Right(ZarrNodeMetadata.Group(group)) =>
        assert(HierarchyIndex.v3(
          ZarrPath.root,
          group,
          HierarchyLimits(maxConsolidatedEntries = 2)
        ).isLeft)
        assert(HierarchyIndex.v3(
          ZarrPath.root,
          group,
          HierarchyLimits(maxConsolidatedNodes = 3)
        ).isLeft)
      case Right(_) => fail("expected group")
      case Left(error) => fail(error.message)

  test("v2 consolidation lowers arrays and infers omitted ancestor groups"):
    val index = zvalue(HierarchyIndex.v2(
      ZarrPath.root,
      HierarchyFixtures.v2Consolidated,
      HierarchyLimits()
    ))
    assertEquals(index.entries.map(entry => entry.path.value -> entry.kind), Vector(
      "" -> NodeKind.Group,
      "bold" -> NodeKind.Array,
      "derived" -> NodeKind.Group,
      "derived/mask" -> NodeKind.Array
    ))
    val bold = index.document(zvalue(ZarrPath("bold"))).getOrElse(fail("missing bold"))
    val descriptor = zvalue(bold.arrayDescriptor(ZarrCapabilities()))
    assertEquals(descriptor.dataType.name, "int16")
    assertEquals(descriptor.dimensionNames, Some(Vector(Some("y"), Some("x"))))

  test("v2 consolidation rejects path conflicts, orphan attributes, and excessive entries"):
    val conflict = HierarchyFixtures.v2Consolidated.replace(
      "\"bold/.zattrs\"",
      "\"bold/.zgroup\":{\"zarr_format\":2},\"bold/.zattrs\""
    )
    assert(HierarchyIndex.v2(ZarrPath.root, conflict, HierarchyLimits()).isLeft)

    val orphan = HierarchyFixtures.v2Consolidated.replace(
      "\"bold/.zattrs\"",
      "\"ghost/.zattrs\":{},\"bold/.zattrs\""
    )
    assert(HierarchyIndex.v2(ZarrPath.root, orphan, HierarchyLimits()).isLeft)

    val arrayAncestor = HierarchyFixtures.v2Consolidated.replace(
      "\"derived/mask/.zarray\"",
      "\"bold/child/.zarray\""
    )
    assert(HierarchyIndex.v2(ZarrPath.root, arrayAncestor, HierarchyLimits()).isLeft)
    assert(HierarchyIndex.v2(
      ZarrPath.root,
      HierarchyFixtures.v2Consolidated,
      HierarchyLimits(maxConsolidatedEntries = 1)
    ).isLeft)
