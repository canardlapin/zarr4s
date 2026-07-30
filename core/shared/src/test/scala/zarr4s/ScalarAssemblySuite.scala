package zarr4s

class ScalarAssemblySuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("scalar formats allocate exact boolean and unsigned fill blocks"):
    val shape = zvalue(Shape(3L))
    val booleans = zvalue(
      PrimitiveBlockBuilder(
        BuiltInDataTypes.bool,
        StoredScalar.Boolean(true),
        shape
      )
    ).result()
    booleans match
      case PrimitiveBlock.Bool(values) =>
        assertEquals(values.toArray.toVector, Vector(true, true, true))
      case found => fail(s"expected bool block, found $found")

    val unsigned = zvalue(
      PrimitiveBlockBuilder(
        BuiltInDataTypes.uint64,
        StoredScalar.UnsignedIntegral(BigInt("18446744073709551615")),
        shape
      )
    ).result()
    unsigned match
      case PrimitiveBlock.UInt64(values) =>
        assertEquals(values.toArray.toVector, Vector(-1L, -1L, -1L))
      case found => fail(s"expected uint64 block, found $found")

  test("block compatibility is derived from the scalar kind, never the data type name"):
    val custom = new DataTypeCapability:
      val name = "lab.uint4"
      val scalarKind = ScalarKind.Unsigned8
      def parseFill(value: JsonValue): Either[String, StoredScalar] =
        Right(StoredScalar.UnsignedIntegral(BigInt(0)))

    val block = PrimitiveBlock.UInt8(OwnedBytes.copyOf(Array[Byte](1, 2)))
    assertEquals(PrimitiveBlockType.validate(block, custom, 2L), Right(()))
    assert(
      PrimitiveBlockType
        .validate(
          PrimitiveBlock.Int8(OwnedBytes.copyOf(Array[Byte](1, 2))),
          custom,
          2L
        )
        .isLeft
    )
    assert(ScalarBytes.encode(block, BuiltInDataTypes.int8, None).isLeft)
