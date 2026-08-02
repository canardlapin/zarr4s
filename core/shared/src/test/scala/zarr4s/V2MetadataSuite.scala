package zarr4s

class V2MetadataSuite extends munit.FunSuite:
  private val cOrder =
    """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"<i2","compressor":null,"fill_value":-1,"order":"C","filters":null}"""

  private def metadata(
      input: String,
      attributes: Option[String] = None
  ): V2ArrayMetadata = V2Metadata.parseArray(input, attributes) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def descriptor(found: V2ArrayMetadata): ArrayDescriptor =
    V2ArrayDescriptor.compile(found) match
      case Right(value) => value
      case Left(error)  => fail(error.message)

  private def codecs(found: ArrayDescriptor): Vector[CompiledCodec] = found.layout match
    case PhysicalLayout.Direct(program) => program.stages
    case _                              => fail("v2 lowering must produce a direct layout")

  test("v2 C-order metadata lowers into the shared descriptor"):
    val found = descriptor(
      metadata(
        cOrder,
        Some(
          """{"_ARRAY_DIMENSIONS":["y","x"],"intent":"BOLD"}"""
        )
      )
    )
    assertEquals(found.shape.toVector, Vector(2L, 3L))
    assertEquals(found.dataType.name, "int16")
    assertEquals(found.fillValue, StoredScalar.Integral(-1L))
    assertEquals(found.chunkKeyEncoding, V2ChunkKeyEncoding(ChunkSeparator.Dot))
    assertEquals(codecs(found).map(_.name), Vector("bytes"))
    assertEquals(found.dimensionNames, Some(Vector(Some("y"), Some("x"))))
    assert(found.attributes.contains("intent"))

  test("v2 Fortran order and endian lower through transpose plus bytes"):
    val input = cOrder
      .replace("<i2", ">i4")
      .replace("\"order\":\"C\"", "\"order\":\"F\"")
      .replace("\"filters\":null", "\"filters\":[],\"dimension_separator\":\"/\"")
    val found = descriptor(metadata(input))
    assertEquals(found.dataType.name, "int32")
    assertEquals(found.chunkKeyEncoding, V2ChunkKeyEncoding(ChunkSeparator.Slash))
    assertEquals(codecs(found).map(_.name), Vector("transpose", "bytes"))
    assertEquals(
      codecs(found).collectFirst { case value: TransposeCodec => value.order },
      Some(Vector(1, 0))
    )
    assertEquals(
      codecs(found).collectFirst { case BytesCodec(order) => order },
      Some(Some(Endianness.Big))
    )

  test("v2 float16 and complex dtypes lower into the shared fixed-width carriers"):
    val half = cOrder
      .replace("<i2", "<f2")
      .replace("\"fill_value\":-1", "\"fill_value\":\"0x3c00\"")
    assertEquals(descriptor(metadata(half)).dataType.name, "float16")
    assertEquals(descriptor(metadata(half)).fillValue, StoredScalar.FloatingBits("0x3c00"))

    val complex = cOrder
      .replace("<i2", ">c8")
      .replace("\"fill_value\":-1", "\"fill_value\":[1,2]")
    val found = descriptor(metadata(complex))
    assertEquals(found.dataType.name, "complex64")
    assertEquals(
      found.fillValue,
      StoredScalar.Complex(StoredFloating.Value(1.0), StoredFloating.Value(2.0))
    )
    assertEquals(
      codecs(found).collectFirst { case value: BytesCodec => value.endianness },
      Some(Some(Endianness.Big))
    )

  test("v2 scalar and rank-five metadata use the same lowering"):
    val scalar = cOrder
      .replace("[2,3]", "[]")
      .replace("\"order\":\"C\"", "\"order\":\"F\"")
    assertEquals(descriptor(metadata(scalar)).shape.rank.toInt, 0)
    assertEquals(codecs(descriptor(metadata(scalar))).map(_.name), Vector("transpose", "bytes"))

    val rankFive = cOrder
      .replace("[2,3]", "[2,1,3,1,4]")
      .replace("\"order\":\"C\"", "\"order\":\"F\"")
    val found = descriptor(metadata(rankFive))
    assertEquals(found.shape.rank.toInt, 5)
    assertEquals(
      codecs(found).collectFirst { case value: TransposeCodec => value.order },
      Some(Vector(4, 3, 2, 1, 0))
    )

  test("v2 zlib and shuffle are executable while unsupported metadata refuses explicitly"):
    assert(V2Metadata.parseArray(cOrder.replace("\"<i2\"", "[[\"x\",\"<i2\"]]"), None).isLeft)
    val zlib = descriptor(
      metadata(
        cOrder.replace(
          "\"compressor\":null",
          "\"compressor\":{\"id\":\"zlib\",\"level\":1}"
        )
      )
    )
    assertEquals(codecs(zlib).map(_.name), Vector("bytes", "zlib"))
    val shuffle = descriptor(
      metadata(
        cOrder.replace(
          "\"filters\":null",
          "\"filters\":[{\"id\":\"shuffle\",\"elementsize\":2}]"
        )
      )
    )
    assertEquals(codecs(shuffle).map(_.name), Vector("bytes", "shuffle"))
    assertEquals(
      codecs(shuffle).collectFirst { case value: ShuffleCodec => value.elementSize },
      Some(2)
    )
    assert(
      V2ArrayDescriptor
        .compile(
          metadata(
            cOrder.replace(
              "\"filters\":null",
              "\"filters\":[{\"id\":\"delta\"}]"
            )
          )
        )
        .isLeft
    )
    assert(
      V2ArrayDescriptor
        .compile(
          metadata(
            cOrder.replace(
              "\"compressor\":null",
              "\"compressor\":{\"id\":\"not-a-codec\",\"level\":1}"
            )
          )
        )
        .isLeft
    )
    assert(
      V2ArrayDescriptor
        .compile(metadata(cOrder.replace("\"fill_value\":-1", "\"fill_value\":null")))
        .isLeft
    )
