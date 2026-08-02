package zarr4s

class ScalarBytesSuite extends munit.FunSuite:
  // Metadata and bytes emitted by Zarr-Python 3.2.1 with NumPy complex64.
  private val zarrPythonComplex64Metadata =
    """{
      "shape":[2],
      "data_type":"complex64",
      "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2]}},
      "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
      "fill_value":[0.0,0.0],
      "codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
      "attributes":{},
      "zarr_format":3,
      "node_type":"array",
      "storage_transformers":[]
    }"""

  private val zarrPythonComplex64Chunk: OwnedBytes =
    ZarrBinaryFixtures.hex("0000c03f000000c0000000800000c07f")

  // Zarr-Python 3.2.1 emits the same direct payload for its raw_bytes extension.
  // The metadata below normalizes that payload to the standard r24 spelling used by this core.
  private val zarrPythonRaw24Metadata =
    """{
      "shape":[2],
      "data_type":"r24",
      "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2]}},
      "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
      "fill_value":[0,0,0],
      "codecs":[{"name":"bytes"}],
      "attributes":{},
      "zarr_format":3,
      "node_type":"array",
      "storage_transformers":[]
    }"""

  private val zarrPythonRaw24Chunk: OwnedBytes =
    ZarrBinaryFixtures.hex("0102ff008007")

  private def block(
      bytes: OwnedBytes,
      dataType: DataTypeCapability,
      order: Endianness,
      count: Long
  ): PrimitiveBlock = ScalarBytes.decode(bytes, dataType, Some(order), count) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(result: Either[CodecError, OwnedBytes]): OwnedBytes = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("Python direct chunk decodes as exact little-endian int16 values"):
    block(
      ZarrBinaryFixtures.directDecodedChunk,
      BuiltInDataTypes.int16,
      Endianness.Little,
      6L
    ) match
      case PrimitiveBlock.Int16(values) =>
        assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
      case _ => fail("expected int16 block")

  test("every common fixed-width scalar format round-trips exact raw bits in both orders"):
    val cases = Vector[(PrimitiveBlock, DataTypeCapability)](
      PrimitiveBlock.Bool(OwnedBooleans.copyOf(Array(false, true))) -> BuiltInDataTypes.bool,
      PrimitiveBlock.Int8(
        OwnedBytes.copyOf(Array[Byte](Byte.MinValue, -1, 0, Byte.MaxValue))
      ) -> BuiltInDataTypes.int8,
      PrimitiveBlock.UInt8(OwnedBytes.copyOf(Array[Byte](0, 1, -1))) -> BuiltInDataTypes.uint8,
      PrimitiveBlock.Int16(
        OwnedShorts.copyOf(Array[Short](Short.MinValue, -1, 0, Short.MaxValue))
      ) -> BuiltInDataTypes.int16,
      PrimitiveBlock.UInt16(OwnedShorts.copyOf(Array[Short](0, 1, -1))) -> BuiltInDataTypes.uint16,
      PrimitiveBlock.Int32(
        OwnedInts.copyOf(Array(Int.MinValue, -1, 0, Int.MaxValue))
      ) -> BuiltInDataTypes.int32,
      PrimitiveBlock.UInt32(OwnedInts.copyOf(Array(0, 1, -1))) -> BuiltInDataTypes.uint32,
      PrimitiveBlock.Int64(
        OwnedLongs.copyOf(Array(Long.MinValue, -1L, 0L, Long.MaxValue))
      ) -> BuiltInDataTypes.int64,
      PrimitiveBlock.UInt64(OwnedLongs.copyOf(Array(0L, 1L, -1L))) -> BuiltInDataTypes.uint64,
      PrimitiveBlock.Float16(OwnedShorts.copyOf(Array[Short](0x3c00.toShort, 0xbc00.toShort))) ->
        BuiltInDataTypes.float16,
      PrimitiveBlock.Float32(
        OwnedFloats.copyOf(Array(Float.NaN, -0.0f, 1.5f, Float.PositiveInfinity))
      ) -> BuiltInDataTypes.float32,
      PrimitiveBlock.Float64(
        OwnedDoubles.copyOf(Array(Double.NaN, -0.0, 1.5, Double.NegativeInfinity))
      ) -> BuiltInDataTypes.float64,
      PrimitiveBlock.Complex64(
        OwnedComplex64.copyOfInterleaved(Array(1.5f, -2.0f, -0.0f, Float.NaN))
      ) -> BuiltInDataTypes.complex64,
      PrimitiveBlock.Complex128(
        OwnedComplex128.copyOfInterleaved(Array(1.5, -2.0, -0.0, Double.NaN))
      ) -> BuiltInDataTypes.complex128,
      PrimitiveBlock.Raw(
        OwnedBytes.copyOf(Array[Byte](0, 1, 2, 253.toByte, 254.toByte, 255.toByte)),
        3
      ) -> BuiltInDataTypes.raw(24).get
    )
    Vector(Endianness.Little, Endianness.Big).foreach: order =>
      cases.foreach: (original, dataType) =>
        val encoded = bytes(ScalarBytes.encode(original, dataType, Some(order)))
        val decoded = block(encoded, dataType, order, original.elementCount.toLong)
        assert(dataType.scalarKind.accepts(decoded))
        assertEquals(bytes(ScalarBytes.encode(decoded, dataType, Some(order))), encoded)

  test("bool decoding rejects non-canonical byte values"):
    assert(
      ScalarBytes
        .decode(
          OwnedBytes.copyOf(Array[Byte](2)),
          BuiltInDataTypes.bool,
          None,
          1L
        )
        .isLeft
    )

  test("scalar byte codec enforces exact decoded length"):
    assert(
      ScalarBytes
        .decode(
          OwnedBytes.copyOf(Array[Byte](1, 0, 2)),
          BuiltInDataTypes.int16,
          Some(Endianness.Little),
          2L
        )
        .isLeft
    )

  test("binary16 conversion preserves canonical values, signed zero, and subnormals"):
    assertEquals(HalfFloat.toBits(1.0f), 0x3c00)
    assertEquals(HalfFloat.toBits(-0.0f), 0x8000)
    assertEquals(HalfFloat.toBits(Float.PositiveInfinity), 0x7c00)
    assertEquals(HalfFloat.toBits(Float.NaN), 0x7e00)
    assertEquals(HalfFloat.fromBits(0x3c00), 1.0f)
    assertEquals(java.lang.Float.floatToRawIntBits(HalfFloat.fromBits(0x8000)), 0x80000000)
    assertEquals(HalfFloat.toBits(HalfFloat.fromBits(0x0001)), 0x0001)

  test("complex and raw public constructors reject malformed storage"):
    assert(OwnedComplex64.fromInterleaved(Array(1.0f)).isLeft)
    assert(OwnedComplex128.fromInterleaved(Array(1.0)).isLeft)
    assert(ScalarKind.raw(0).isLeft)
    assert(ScalarKind.raw(3).isRight)
    assert(PrimitiveBlock.raw(OwnedBytes.copyOf(Array[Byte](1, 2)), 0).isLeft)
    assert(PrimitiveBlock.raw(OwnedBytes.copyOf(Array[Byte](1, 2)), 3).isLeft)

  test("complex and raw blocks retain element boundaries during selection operations"):
    val source = PrimitiveBlock.Complex64(
      OwnedComplex64.copyOfInterleaved(Array(1.0f, 2.0f, 3.0f, 4.0f))
    )
    val target = PrimitiveBlock.Complex64(
      OwnedComplex64.copyOfInterleaved(Array(0.0f, 0.0f))
    )
    assertEquals(target.copyElementFrom(source, 1, 0), Right(()))
    target match
      case PrimitiveBlock.Complex64(values) =>
        assertEquals(values.toInterleavedArray.toVector, Vector(3.0f, 4.0f))
      case _ => fail("expected complex64 block")

    val raw = PrimitiveBlock.Raw(OwnedBytes.copyOf(Array[Byte](1, 2, 3, 4)), 2)
    raw.reordered(Array(1, 0)) match
      case PrimitiveBlock.Raw(values, width) =>
        assertEquals(width, 2)
        assertEquals(values.toArray.toVector, Vector[Byte](3, 4, 1, 2))
      case _ => fail("expected raw block")

  test("complex transpose preserves pair boundaries and inverts exactly"):
    val original = PrimitiveBlock.Complex64(
      OwnedComplex64.copyOfInterleaved(
        Array(0.0f, 10.0f, 1.0f, 11.0f, 2.0f, 12.0f, 3.0f, 13.0f)
      )
    )
    val transpose = TransposeCodec.from(Vector(1, 0)) match
      case Right(found) => found
      case Left(detail) => fail(detail)
    val shape = Shape(2L, 2L) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    val encoded = transpose.encodeArray(original, shape) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    encoded.block match
      case PrimitiveBlock.Complex64(values) =>
        assertEquals(
          values.toInterleavedArray.toVector,
          Vector(0.0f, 10.0f, 2.0f, 12.0f, 1.0f, 11.0f, 3.0f, 13.0f)
        )
      case _ => fail("expected complex64 block")
    transpose.decodeArray(encoded.block, shape) match
      case Right(PrimitiveBlock.Complex64(values)) =>
        val expected = original match
          case PrimitiveBlock.Complex64(found) => found.toInterleavedArray.toVector
          case _                               => Vector.empty
        assertEquals(values.toInterleavedArray.toVector, expected)
      case Right(_)    => fail("expected complex64 block")
      case Left(error) => fail(error.message)

  test("fill allocation uses exact fixed-width component and raw-byte semantics"):
    BuiltInDataTypes.float16.scalarKind.allocate(
      StoredScalar.Floating(1.0),
      2,
      "float16"
    ) match
      case Right(PrimitiveBlock.Float16(values)) =>
        assertEquals(values.toArray.toVector, Vector[Short](0x3c00, 0x3c00))
      case Right(_)    => fail("expected float16 fill block")
      case Left(error) => fail(error.message)

    BuiltInDataTypes.complex64.scalarKind.allocate(
      StoredScalar.Complex(StoredFloating.Bits("0x3f800000"), StoredFloating.Value(-2.0)),
      2,
      "complex64"
    ) match
      case Right(PrimitiveBlock.Complex64(values)) =>
        assertEquals(java.lang.Float.floatToRawIntBits(values.real(0)), 0x3f800000)
        assertEquals(java.lang.Float.floatToRawIntBits(values.imaginary(0)), 0xc0000000)
        assertEquals(values.length, 2)
      case Right(_)    => fail("expected complex64 fill block")
      case Left(error) => fail(error.message)

    BuiltInDataTypes
      .raw(24)
      .get
      .scalarKind
      .allocate(
        StoredScalar.RawBytes(Vector(1, 2, 255)),
        2,
        "r24"
      ) match
      case Right(PrimitiveBlock.Raw(values, width)) =>
        assertEquals(width, 3)
        assertEquals(values.toArray.toVector, Vector[Byte](1, 2, -1, 1, 2, -1))
      case Right(_)    => fail("expected raw fill block")
      case Left(error) => fail(error.message)

    assert(
      BuiltInDataTypes
        .raw(24)
        .get
        .scalarKind
        .allocate(StoredScalar.RawBytes(Vector(256, 0, 0)), 1, "r24")
        .isLeft
    )

  test("Zarr-Python complex64 fixture preserves component order and exact bits"):
    val metadata = ZarrMetadata.parse(zarrPythonComplex64Metadata) match
      case Right(ZarrNodeMetadata.Array(found)) => found
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)
    val descriptor = ArrayDescriptor.compile(metadata) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    ScalarBytes.decode(
      zarrPythonComplex64Chunk,
      descriptor.dataType,
      Some(Endianness.Little),
      2L
    ) match
      case Right(PrimitiveBlock.Complex64(values)) =>
        assertEquals(java.lang.Float.floatToRawIntBits(values.real(0)), 0x3fc00000)
        assertEquals(java.lang.Float.floatToRawIntBits(values.imaginary(0)), 0xc0000000)
        assertEquals(java.lang.Float.floatToRawIntBits(values.real(1)), 0x80000000)
        assertEquals(java.lang.Float.floatToRawIntBits(values.imaginary(1)), 0x7fc00000)
      case Right(_)    => fail("expected complex64 block")
      case Left(error) => fail(error.message)

  test("Zarr-Python raw byte payload decodes through the standard r24 carrier"):
    val metadata = ZarrMetadata.parse(zarrPythonRaw24Metadata) match
      case Right(ZarrNodeMetadata.Array(found)) => found
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)
    val descriptor = ArrayDescriptor.compile(metadata) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    ScalarBytes.decode(
      zarrPythonRaw24Chunk,
      descriptor.dataType,
      None,
      2L
    ) match
      case Right(PrimitiveBlock.Raw(values, width)) =>
        assertEquals(width, 3)
        assertEquals(values.toArray.toVector, Vector[Byte](1, 2, -1, 0, -128, 7))
      case Right(_)    => fail("expected raw block")
      case Left(error) => fail(error.message)
