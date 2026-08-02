package zarr4s

class MetadataSuite extends munit.FunSuite:
  private def metadata(input: String): ZarrNodeMetadata = ZarrMetadata.parse(input) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def array(input: String): ArrayMetadata = metadata(input) match
    case ZarrNodeMetadata.Array(found) => found
    case _                             => fail("expected array metadata")

  private def descriptor(input: String): ArrayDescriptor =
    ArrayDescriptor.compile(array(input)) match
      case Right(found) => found
      case Left(error)  => fail(error.message)

  private def directCodecs(found: ArrayDescriptor): Vector[CompiledCodec] = found.layout match
    case PhysicalLayout.Direct(codecs)         => codecs.stages
    case PhysicalLayout.Sharded(_, _, _, _, _) => fail("expected direct layout")

  private val valid =
    """{
      "zarr_format": 3,
      "node_type": "array",
      "shape": [1200, 72, 96, 96],
      "data_type": "float32",
      "chunk_grid": {"name":"regular","configuration":{"chunk_shape":[16,24,32,32]}},
      "chunk_key_encoding": {"name":"default","configuration":{"separator":"/"}},
      "fill_value": "NaN",
      "codecs": [
        {"name":"bytes","configuration":{"endian":"little"}},
        {"name":"gzip","configuration":{"level":1}},
        "crc32c"
      ],
      "dimension_names": ["time", "z", "y", "x"],
      "attributes": {"intent":"BOLD"},
      "future_field": {"kept":true}
    }"""

  test("array metadata compiles into an executable runtime-rank descriptor"):
    val found = descriptor(valid)
    assertEquals(found.shape.toVector, Vector(1200L, 72L, 96L, 96L))
    assertEquals(found.grid.chunkShape.toVector, Vector(16L, 24L, 32L, 32L))
    assertEquals(found.grid.gridShape.toVector, Vector(75L, 3L, 3L, 3L))
    assertEquals(found.dataType.name, "float32")
    assertEquals(directCodecs(found).map(_.name), Vector("bytes", "gzip", "crc32c"))
    assertEquals(found.dimensionNames, Some(Vector(Some("time"), Some("z"), Some("y"), Some("x"))))
    assert(found.unknown.contains("future_field"))

  test("group metadata and unknown fields are preserved"):
    metadata("""{"zarr_format":3,"node_type":"group","attributes":{"a":1},"future":42}""") match
      case ZarrNodeMetadata.Group(group) =>
        assert(group.attributes.contains("a"))
        assert(group.unknown.contains("future"))
      case _ => fail("expected group metadata")

  test("scalar and rank-five arrays use the same compiler"):
    val scalar = valid
      .replace("[1200, 72, 96, 96]", "[]")
      .replace("[16,24,32,32]", "[]")
      .replace("[\"time\", \"z\", \"y\", \"x\"]", "[]")
    assertEquals(descriptor(scalar).shape.rank.toInt, 0)

    val rankFive = valid
      .replace("[1200, 72, 96, 96]", "[2,1200,72,96,96]")
      .replace("[16,24,32,32]", "[1,16,24,32,32]")
      .replace("[\"time\", \"z\", \"y\", \"x\"]", "[\"echo\",\"time\",\"z\",\"y\",\"x\"]")
    assertEquals(descriptor(rankFive).shape.rank.toInt, 5)

  test("metadata compiler rejects mismatched named dimensions"):
    val invalid = valid.replace(
      "[\"time\", \"z\", \"y\", \"x\"]",
      "[\"time\", \"voxel\"]"
    )
    assert(ZarrMetadata.parse(invalid).isLeft)

  test("multibyte bytes codec requires explicit endianness"):
    val invalid = valid.replace(
      "{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}",
      "\"bytes\""
    )
    assert(ArrayDescriptor.compile(array(invalid)).isLeft)

  test("unsupported required extensions fail while ignorable ones are retained safely"):
    val required = valid.replace(
      "\"crc32c\"",
      "{\"name\":\"future-codec\"}"
    )
    assert(ArrayDescriptor.compile(array(required)).isLeft)

    val ignorable = valid.replace(
      "\"crc32c\"",
      "{\"name\":\"future-codec\",\"must_understand\":false}"
    )
    assertEquals(directCodecs(descriptor(ignorable)).map(_.name), Vector("bytes", "gzip"))

  test("capabilities are open to downstream data types"):
    val custom = new DataTypeCapability:
      val name = "lab.uint4"
      val scalarKind = ScalarKind.Unsigned8
      def parseFill(value: JsonValue): Either[String, StoredScalar] = value match
        case JsonValue.Num(number) =>
          number.toLongExact.flatMap: found =>
            if found >= 0L && found <= 15L then Right(StoredScalar.Integral(found))
            else Left("uint4 fill must be in [0, 15]")
        case _ => Left("uint4 fill must be an integer")

    val input = valid
      .replace("\"float32\"", "\"lab.uint4\"")
      .replace("\"NaN\"", "7")
      .replace("{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}", "\"bytes\"")
    val compiled = ArrayDescriptor.compile(
      array(input),
      ZarrCapabilities(dataTypes = BuiltInDataTypes.all :+ custom)
    )
    assertEquals(compiled.map(_.dataType.name), Right("lab.uint4"))

  test("common fixed-width data types expose executable scalar formats"):
    assertEquals(
      BuiltInDataTypes.all.map(dataType => dataType.name -> dataType.byteWidth),
      Vector(
        "bool" -> 1,
        "int8" -> 1,
        "int16" -> 2,
        "int32" -> 4,
        "int64" -> 8,
        "uint8" -> 1,
        "uint16" -> 2,
        "uint32" -> 4,
        "uint64" -> 8,
        "float16" -> 2,
        "float32" -> 4,
        "float64" -> 8,
        "complex64" -> 8,
        "complex128" -> 16
      )
    )

  test("float16, complex, and raw fills compile with exact representations"):
    val half = valid
      .replace("\"float32\"", "\"float16\"")
      .replace("\"NaN\"", "\"0x3c00\"")
    assertEquals(descriptor(half).fillValue, StoredScalar.FloatingBits("0x3c00"))

    val complex = valid
      .replace("\"float32\"", "\"complex64\"")
      .replace("\"NaN\"", "[1.5,\"0xc0000000\"]")
    assertEquals(
      descriptor(complex).fillValue,
      StoredScalar.Complex(
        StoredFloating.Value(1.5),
        StoredFloating.Bits("0xc0000000")
      )
    )

    val raw = valid
      .replace("\"float32\"", "\"r24\"")
      .replace("\"NaN\"", "[1,2,255]")
    val rawDescriptor = descriptor(raw)
    assertEquals(rawDescriptor.dataType.byteWidth, 3)
    assertEquals(rawDescriptor.fillValue, StoredScalar.RawBytes(Vector(1, 2, 255)))
    val rendered =
      ZarrMetadataRenderer.array(rawDescriptor).fold(error => fail(error.message), identity)
    assert(rendered.contains("\"data_type\":\"r24\""))
    assert(rendered.contains("\"fill_value\":[1,2,255]"))

  test("dynamic raw widths are restricted to positive whole bytes"):
    assert(BuiltInDataTypes.raw(0).isEmpty)
    assert(BuiltInDataTypes.raw(7).isEmpty)
    assert(BuiltInDataTypes.raw(24).nonEmpty)

  test("boolean and unsigned fill values are exact and range checked"):
    val bool = valid
      .replace("\"float32\"", "\"bool\"")
      .replace("\"NaN\"", "true")
      .replace("{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}", "\"bytes\"")
    assertEquals(descriptor(bool).fillValue, StoredScalar.Boolean(true))

    val maximum = valid
      .replace("\"float32\"", "\"uint64\"")
      .replace("\"NaN\"", "18446744073709551615")
    assertEquals(
      descriptor(maximum).fillValue,
      StoredScalar.UnsignedIntegral(BigInt("18446744073709551615"))
    )
    assert(
      ArrayDescriptor
        .compile(
          array(
            maximum.replace(
              "18446744073709551615",
              "18446744073709551616"
            )
          )
        )
        .isLeft
    )

  test("transpose metadata is shape checked at descriptor construction"):
    val transposed = valid.replace(
      "{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}",
      "{\"name\":\"transpose\",\"configuration\":{\"order\":[3,2,1,0]}},{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}"
    )
    assertEquals(directCodecs(descriptor(transposed)).map(_.name).head, "transpose")
    assert(ArrayDescriptor.compile(array(transposed.replace("[3,2,1,0]", "[1,0]"))).isLeft)

  test("v2 chunk keys default to dot and survive deterministic rendering"):
    val input = valid.replace(
      "{\"name\":\"default\",\"configuration\":{\"separator\":\"/\"}}",
      "{\"name\":\"v2\"}"
    )
    val found = descriptor(input)
    assertEquals(found.chunkKeyEncoding, V2ChunkKeyEncoding(ChunkSeparator.Dot))
    val rendered = ZarrMetadataRenderer.array(found).fold(error => fail(error.message), identity)
    assert(
      rendered.contains(
        "\"chunk_key_encoding\":{\"configuration\":{\"separator\":\".\"},\"name\":\"v2\"}"
      )
    )

  test("integer fill values are exact and range checked"):
    val int16 = valid
      .replace("\"float32\"", "\"int16\"")
      .replace("\"NaN\"", "32768")
    assert(ArrayDescriptor.compile(array(int16)).isLeft)
