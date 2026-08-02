package zarr4s

class DescriptorFactorySuite extends munit.FunSuite:
  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def arrayMetadata(input: String): ArrayMetadata =
    ZarrMetadata.parse(input) match
      case Right(ZarrNodeMetadata.Array(found)) => found
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)

  private def descriptor(input: String): ArrayDescriptor =
    value(ArrayDescriptor.compile(arrayMetadata(input)))

  private def directCodecs(found: ArrayDescriptor): Vector[String] = found.layout match
    case PhysicalLayout.Direct(codecs)         => codecs.stages.map(_.name)
    case PhysicalLayout.Sharded(_, _, _, _, _) => fail("expected direct layout")

  test("programmatic v3 direct descriptors match the canonical Python metadata"):
    val shape = value(Shape(7L))
    val chunks = value(Shape(3L))
    val base = value(ArraySpec(DType.Float32, shape, chunks)).withFill(Float.NaN)
    val spec = value(base.withDimensionNames(Vector(Some("x"))))
    val found = value(ArrayDescriptor.direct(spec))
    val expected = descriptor(ZarrPythonFixtures.rank1)

    assertEquals(found.shape, expected.shape)
    assertEquals(found.grid.chunkShape, expected.grid.chunkShape)
    assertEquals(found.dataType.name, expected.dataType.name)
    (found.fillValue, expected.fillValue) match
      case (StoredScalar.Floating(left), StoredScalar.Floating(right))
          if left.isNaN && right.isNaN =>
        ()
      case (left, right) => assertEquals(left, right)
    assertEquals(found.dimensionNames, expected.dimensionNames)
    assertEquals(found.attributes, expected.attributes)
    assertEquals(directCodecs(found), Vector("bytes"))
    assertEquals(found.chunkKeyEncoding.separator, ChunkSeparator.Slash)

  test("programmatic v3 direct descriptors cover scalar, empty, and rank five"):
    val cases = Vector(
      (value(Shape()), value(Shape())),
      (value(Shape(2L, 0L)), value(Shape(4L, 3L))),
      (value(Shape(1L, 2L, 3L, 4L, 5L)), value(Shape(1L, 1L, 2L, 2L, 3L)))
    )
    cases.foreach: (shape, chunks) =>
      val spec = value(ArraySpec(DType.Int16, shape, chunks))
      val found = value(ArrayDescriptor.direct(spec))
      assertEquals(found.shape, shape)
      assertEquals(found.grid.chunkShape, chunks)
      assertEquals(directCodecs(found), Vector("bytes"))

  test("programmatic v3 direct descriptors accept typed fills and explicit codecs"):
    val shape = value(Shape(8L, 8L))
    val chunks = value(Shape(4L, 4L))
    val spec = value(ArraySpec(DType.Int16, shape, chunks)).withFill(7.toShort)
    val found = value(
      ArrayDescriptor.direct(
        spec,
        codecs = Vector(
          ArrayCodecSpec.Bytes.little,
          ArrayCodecSpec.Gzip(1),
          ArrayCodecSpec.Crc32c
        )
      )
    )
    assertEquals(found.fillValue, StoredScalar.Integral(7L))
    assertEquals(directCodecs(found), Vector("bytes", "gzip", "crc32c"))

  test("programmatic v3 sharding matches the independent zarrs descriptor"):
    val spec = value(
      ArraySpec(
        DType.UInt16,
        value(Shape(8L, 8L)),
        value(Shape(4L, 8L))
      )
    )
    val sharding = ShardingSpec.indexed(
      value(Shape(4L, 4L)),
      innerCodecs = Vector(ArrayCodecSpec.Bytes.little, ArrayCodecSpec.Gzip(5)),
      indexCodecs = Vector(ArrayCodecSpec.Bytes.little, ArrayCodecSpec.Crc32c),
      indexLocation = IndexLocation.End
    )
    val found = value(ArrayDescriptor.sharded(spec, sharding))
    val expected = descriptor(ZarrsFixtures.shardedMetadata)

    assertEquals(found.shape, expected.shape)
    assertEquals(found.grid.chunkShape, expected.grid.chunkShape)
    assertEquals(found.dataType.name, expected.dataType.name)
    assertEquals(found.fillValue, expected.fillValue)
    assertEquals(found.chunkKeyEncoding.separator, expected.chunkKeyEncoding.separator)
    (found.layout, expected.layout) match
      case (
            PhysicalLayout.Sharded(foundGrid, foundInner, foundIndex, foundLocation, foundOuter),
            PhysicalLayout.Sharded(
              expectedGrid,
              expectedInner,
              expectedIndex,
              expectedLocation,
              expectedOuter
            )
          ) =>
        assertEquals(foundGrid.innerChunkShape, expectedGrid.innerChunkShape)
        assertEquals(foundInner.stages.map(_.name), expectedInner.stages.map(_.name))
        assertEquals(foundIndex.codecs.stages.map(_.name), expectedIndex.codecs.stages.map(_.name))
        assertEquals(foundLocation, expectedLocation)
        assertEquals(foundOuter.stages.map(_.name), expectedOuter.stages.map(_.name))
      case _ => fail("expected two sharded descriptors")

  test("programmatic v2 descriptors lower through the existing v2 compiler"):
    val spec = value(
      ArraySpec(
        DType.Int16,
        value(Shape(2L, 3L)),
        value(Shape(2L, 3L))
      )
    ).asFormat(ZarrFormat.V2)
    val found = value(ArrayDescriptor.direct(spec))
    val expected = value(
      V2ArrayDescriptor.compile(
        value(
          V2Metadata.parseArray(
            """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"<i2","compressor":null,"fill_value":0,"order":"C","filters":null,"dimension_separator":"."}"""
          )
        )
      )
    )
    assertEquals(found.shape, expected.shape)
    assertEquals(found.grid.chunkShape, expected.grid.chunkShape)
    assertEquals(found.dataType.name, expected.dataType.name)
    assertEquals(found.chunkKeyEncoding.separator, expected.chunkKeyEncoding.separator)
    assertEquals(directCodecs(found), directCodecs(expected))

  test("factory preserves the canonical renderer without a metadata round trip"):
    val spec = value(
      ArraySpec(
        DType.Int16,
        value(Shape(2L, 3L)),
        value(Shape(2L, 3L))
      )
    )
    val found = value(ArrayDescriptor.direct(spec))
    val rendered = value(ZarrMetadataRenderer.array(found))
    val parsed = descriptor(rendered)
    assertEquals(parsed.shape, found.shape)
    assertEquals(parsed.grid.chunkShape, found.grid.chunkShape)
    assertEquals(parsed.grid.gridShape, found.grid.gridShape)
    assertEquals(parsed.dataType.name, found.dataType.name)
    assertEquals(parsed.fillValue, found.fillValue)
    assertEquals(parsed.chunkKeyEncoding, found.chunkKeyEncoding)
    assertEquals(parsed.layout, found.layout)
    assertEquals(parsed.dimensionNames, found.dimensionNames)
    assertEquals(parsed.attributes, found.attributes)

  test("factory rejects sharding in v2 and invalid index pipelines explicitly"):
    val spec = value(
      ArraySpec(
        DType.Int16,
        value(Shape(8L, 8L)),
        value(Shape(4L, 8L))
      )
    ).asFormat(ZarrFormat.V2)
    val sharding = ShardingSpec.indexed(value(Shape(4L, 4L)))
    assertEquals(
      ArrayDescriptor.sharded(spec, sharding),
      Left(ZarrError.UnsupportedWrite("indexed sharding is a Zarr v3 descriptor feature"))
    )

    val invalid = ShardingSpec.indexed(
      value(Shape(4L, 4L)),
      indexCodecs = Vector(ArrayCodecSpec.Gzip(1))
    )
    assert(
      ArrayDescriptor
        .sharded(
          value(
            ArraySpec(
              DType.Int16,
              value(Shape(8L, 8L)),
              value(Shape(4L, 8L))
            )
          ),
          invalid
        )
        .isLeft
    )
