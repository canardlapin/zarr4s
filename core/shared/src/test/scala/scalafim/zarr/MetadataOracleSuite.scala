package scalafim.zarr

class MetadataOracleSuite extends munit.FunSuite:
  test("Zarr-Python 3.2.1 metadata compiles at ranks 0, 1, 2, 4, and 5"):
    ZarrPythonFixtures.all.foreach: (rank, input) =>
      val compiled = for
        node <- ZarrMetadata.parse(input)
        array <- node match
          case ZarrNodeMetadata.Array(metadata) => Right(metadata)
          case _ => Left(ZarrError.UnsupportedNodeType("group"))
        descriptor <- ArrayDescriptor.compile(array)
      yield descriptor
      assertEquals(compiled.map(_.shape.rank.toInt), Right(rank), s"rank $rank")

  test("mutated Python fixtures fail at the responsible typed boundary"):
    val wrongRank = ZarrPythonFixtures.rank4.replace(
      "[\"time\",\"z\",\"y\",\"x\"]",
      "[\"time\",\"voxel\"]"
    )
    assert(ZarrMetadata.parse(wrongRank) match
      case Left(ZarrError.InvalidMetadata("$.dimension_names", _)) => true
      case _ => false
    )

    val unsupportedCodec = ZarrPythonFixtures.rank4.replace(
      "\"name\":\"bytes\"",
      "\"name\":\"zstd\""
    )
    val result = ZarrMetadata.parse(unsupportedCodec).flatMap:
      case ZarrNodeMetadata.Array(metadata) => ArrayDescriptor.compile(metadata)
      case _ => Left(ZarrError.UnsupportedNodeType("group"))
    assertEquals(result, Left(ZarrError.UnsupportedExtension("codec", "zstd")))

  test("Zarr-Python sharding metadata compiles into a physical layout"):
    val compiled = for
      node <- ZarrMetadata.parse(ZarrPythonFixtures.shardedRank2)
      array <- node match
        case ZarrNodeMetadata.Array(metadata) => Right(metadata)
        case _ => Left(ZarrError.UnsupportedNodeType("group"))
      descriptor <- ArrayDescriptor.compile(array)
    yield descriptor

    compiled match
      case Right(ArrayDescriptor(_, _, _, outerGrid, _, PhysicalLayout.Sharded(
        sharded,
        innerCodecs,
        indexCodecs,
        location,
        outerCodecs
      ), _, _, _)) =>
        assertEquals(outerGrid.chunkShape.toVector, Vector(4L, 4L))
        assertEquals(sharded.innerChunkShape.toVector, Vector(2L, 2L))
        assertEquals(sharded.innerChunksPerShard.toVector, Vector(2L, 2L))
        assertEquals(innerCodecs.stages.map(_.name), Vector("bytes"))
        assertEquals(indexCodecs.codecs.stages.map(_.name), Vector("bytes", "crc32c"))
        assertEquals(location, IndexLocation.End)
        assertEquals(outerCodecs.stages, Vector.empty)
      case Right(_) => fail("expected sharded physical layout")
      case Left(error) => fail(error.message)

  test("deterministic metadata rendering round-trips direct and sharded descriptors"):
    Vector(ZarrPythonFixtures.rank5, ZarrBinaryFixtures.shardedStartMetadata).foreach: input =>
      val original = for
        node <- ZarrMetadata.parse(input)
        array <- node match
          case ZarrNodeMetadata.Array(metadata) => Right(metadata)
          case _ => Left(ZarrError.UnsupportedNodeType("group"))
        descriptor <- ArrayDescriptor.compile(array)
      yield descriptor
      val rendered = original.flatMap(ZarrMetadataRenderer.array)
      val reparsed = rendered.flatMap(text => ZarrMetadata.parse(text)).flatMap:
        case ZarrNodeMetadata.Array(metadata) => ArrayDescriptor.compile(metadata)
        case _ => Left(ZarrError.UnsupportedNodeType("group"))
      assertEquals(reparsed.map(_.shape), original.map(_.shape))
      assertEquals(rendered, rendered.flatMap(text =>
        ZarrMetadata.parse(text).flatMap:
          case ZarrNodeMetadata.Array(metadata) =>
            ArrayDescriptor.compile(metadata).flatMap(ZarrMetadataRenderer.array)
          case _ => Left(ZarrError.UnsupportedNodeType("group"))
      ))
