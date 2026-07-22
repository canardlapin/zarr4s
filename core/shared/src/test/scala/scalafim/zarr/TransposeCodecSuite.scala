package scalafim.zarr

class TransposeCodecSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def codec(order: Int*): TransposeCodec = TransposeCodec.from(order) match
    case Right(found) => found
    case Left(detail) => fail(detail)

  private def ints(block: PrimitiveBlock): Vector[Int] = block match
    case PrimitiveBlock.Int32(values) => values.toArray.toVector
    case _ => fail("expected int32 block")

  test("transpose follows the normative shape and position permutation"):
    val original = PrimitiveBlock.Int32(OwnedInts.copyOf(Array(0, 1, 2, 3, 4, 5)))
    val shape = zvalue(Shape(2L, 3L))
    val transposed = zvalue(codec(1, 0).encodeArray(original, shape))
    assertEquals(transposed.shape, zvalue(Shape(3L, 2L)))
    assertEquals(ints(transposed.block), Vector(0, 3, 1, 4, 2, 5))
    assertEquals(ints(zvalue(codec(1, 0).decodeArray(transposed.block, shape))), ints(original))

  test("transpose and its inverse are rank-generic, including scalar and rank five"):
    val scalar = PrimitiveBlock.Int32(OwnedInts.copyOf(Array(7)))
    val scalarResult = zvalue(codec().encodeArray(scalar, zvalue(Shape())))
    assertEquals(scalarResult.shape, zvalue(Shape()))
    assertEquals(ints(scalarResult.block), Vector(7))

    val shape = zvalue(Shape(2L, 1L, 2L, 1L, 3L))
    val original = PrimitiveBlock.Int32(OwnedInts.copyOf(Array.range(0, 12)))
    val rankFive = codec(4, 0, 2, 1, 3)
    val encoded = zvalue(rankFive.encodeArray(original, shape))
    assertEquals(encoded.shape, zvalue(Shape(3L, 2L, 2L, 1L, 1L)))
    assertEquals(ints(zvalue(rankFive.decodeArray(encoded.block, shape))), ints(original))

  test("transpose rejects invalid permutations and block-shape mismatches"):
    assert(TransposeCodec.from(Vector(0, 0)).isLeft)
    assert(TransposeCodec.from(Vector(0, 2)).isLeft)
    assert(codec(1, 0).encodedShape(zvalue(Shape(2L, 3L, 4L))).isLeft)
    assert(codec(1, 0).encodeArray(
      PrimitiveBlock.Int32(OwnedInts.copyOf(Array(1, 2))),
      zvalue(Shape(2L, 2L))
    ).isLeft)

  test("codec runtime composes transpose with bytes in both directions"):
    val transpose = codec(1, 0)
    val program = zvalue(CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(transpose, BytesCodec(Some(Endianness.Big)))
    ))
    val shape = zvalue(Shape(2L, 3L))
    val original = PrimitiveBlock.Int32(OwnedInts.copyOf(Array(0, 1, 2, 3, 4, 5)))
    val encoded = zvalue(SyncCodecRuntime.core.encode(
      original,
      BuiltInDataTypes.int32,
      shape,
      program,
      ByteCount.unsafe(1024L)
    ))
    val decoded = zvalue(SyncCodecRuntime.core.decode(
      encoded,
      program,
      BuiltInDataTypes.int32,
      shape,
      DecodeLimits.default
    ))
    assertEquals(ints(decoded), ints(original))
