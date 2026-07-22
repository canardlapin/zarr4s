package scalafim.zarr

class ScalarBytesSuite extends munit.FunSuite:
  private def block(
      bytes: OwnedBytes,
      dataType: DataTypeCapability,
      order: Endianness,
      count: Long
  ): PrimitiveBlock = ScalarBytes.decode(bytes, dataType, Some(order), count) match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def bytes(result: Either[CodecError, OwnedBytes]): OwnedBytes = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

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
      PrimitiveBlock.Int8(OwnedBytes.copyOf(Array[Byte](Byte.MinValue, -1, 0, Byte.MaxValue))) -> BuiltInDataTypes.int8,
      PrimitiveBlock.UInt8(OwnedBytes.copyOf(Array[Byte](0, 1, -1))) -> BuiltInDataTypes.uint8,
      PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](Short.MinValue, -1, 0, Short.MaxValue))) -> BuiltInDataTypes.int16,
      PrimitiveBlock.UInt16(OwnedShorts.copyOf(Array[Short](0, 1, -1))) -> BuiltInDataTypes.uint16,
      PrimitiveBlock.Int32(OwnedInts.copyOf(Array(Int.MinValue, -1, 0, Int.MaxValue))) -> BuiltInDataTypes.int32,
      PrimitiveBlock.UInt32(OwnedInts.copyOf(Array(0, 1, -1))) -> BuiltInDataTypes.uint32,
      PrimitiveBlock.Int64(OwnedLongs.copyOf(Array(Long.MinValue, -1L, 0L, Long.MaxValue))) -> BuiltInDataTypes.int64,
      PrimitiveBlock.UInt64(OwnedLongs.copyOf(Array(0L, 1L, -1L))) -> BuiltInDataTypes.uint64,
      PrimitiveBlock.Float32(OwnedFloats.copyOf(Array(Float.NaN, -0.0f, 1.5f, Float.PositiveInfinity))) -> BuiltInDataTypes.float32,
      PrimitiveBlock.Float64(OwnedDoubles.copyOf(Array(Double.NaN, -0.0, 1.5, Double.NegativeInfinity))) -> BuiltInDataTypes.float64
    )
    Vector(Endianness.Little, Endianness.Big).foreach: order =>
      cases.foreach: (original, dataType) =>
        val encoded = bytes(ScalarBytes.encode(original, dataType, Some(order)))
        val decoded = block(encoded, dataType, order, original.elementCount.toLong)
        assert(dataType.scalarKind.accepts(decoded))
        assertEquals(bytes(ScalarBytes.encode(decoded, dataType, Some(order))), encoded)

  test("bool decoding rejects non-canonical byte values"):
    assert(ScalarBytes.decode(
      OwnedBytes.copyOf(Array[Byte](2)),
      BuiltInDataTypes.bool,
      None,
      1L
    ).isLeft)

  test("scalar byte codec enforces exact decoded length"):
    assert(ScalarBytes.decode(
      OwnedBytes.copyOf(Array[Byte](1, 0, 2)),
      BuiltInDataTypes.int16,
      Some(Endianness.Little),
      2L
    ).isLeft)
