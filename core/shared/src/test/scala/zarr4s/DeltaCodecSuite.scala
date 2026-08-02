package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DeltaCodecSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def stringValue[A](result: Either[String, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error)

  private def shape(size: Long): Shape = zvalue(Shape(size))

  private def codec(
      dataType: DataTypeCapability,
      dtype: String,
      astype: Option[String] = None
  ): DeltaCodec =
    val fields = Vector(
      "dtype" -> JsonValue.Str(dtype)
    ) ++ astype.map(value => "astype" -> JsonValue.Str(value)).toVector
    stringValue(
      DeltaCodec.fromConfiguration(JsonObject.unsafe(fields), dataType)
    )

  private def int16(block: PrimitiveBlock): Vector[Short] = block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 block")

  private def int8(block: PrimitiveBlock): Vector[Byte] = block match
    case PrimitiveBlock.Int8(values) => values.toArray.toVector
    case _                           => fail("expected int8 block")

  private def bools(block: PrimitiveBlock): Vector[Boolean] = block match
    case PrimitiveBlock.Bool(values) => values.toArray.toVector
    case _                           => fail("expected bool block")

  private def program(codec: DeltaCodec): CodecProgram =
    zvalue(
      CodecProgram.compile(
        CodecRepresentation.ArrayValues,
        Vector(codec, BytesCodec(codec.encodedEndianness), ShuffleCodec(1))
      )
    )

  test("delta encodes adjacent integer differences and reverses astype"):
    val original = PrimitiveBlock.Int16(
      OwnedShorts.copyOf(Array[Short](100, 102, 98, 99))
    )
    val delta = codec(BuiltInDataTypes.int16, "<i2", Some("|i1"))
    val encoded = zvalue(delta.encodeArray(original, shape(4L)))
    assertEquals(int8(encoded.block), Vector[Byte](100, 2, -4, 1))
    assertEquals(int16(zvalue(delta.decodeArray(encoded.block, shape(4L)))), int16(original))

  test("delta preserves fixed-width integer overflow semantics"):
    val original = PrimitiveBlock.Int8(
      OwnedBytes.copyOf(Array[Byte](-128, 127, -128))
    )
    val delta = codec(BuiltInDataTypes.int8, "|i1")
    val encoded = zvalue(delta.encodeArray(original, shape(3L)))
    assertEquals(int8(encoded.block), Vector[Byte](-128, -1, 1))
    assertEquals(
      zvalue(delta.decodeArray(encoded.block, shape(3L))) match
        case PrimitiveBlock.Int8(values) => values.toArray.toVector
        case _                           => fail("expected int8 block"),
      Vector[Byte](-128, 127, -128)
    )

  test("delta supports boolean transitions, floats, and empty chunks"):
    val boolDelta = codec(BuiltInDataTypes.bool, "|b1")
    val booleans = PrimitiveBlock.Bool(OwnedBooleans.copyOf(Array(true, true, false, true)))
    val encodedBooleans = zvalue(boolDelta.encodeArray(booleans, shape(4L)))
    assertEquals(bools(encodedBooleans.block), Vector(true, false, true, true))
    assertEquals(
      bools(zvalue(boolDelta.decodeArray(encodedBooleans.block, shape(4L)))),
      bools(booleans)
    )

    val floatDelta = codec(BuiltInDataTypes.float32, "<f4", Some("<f2"))
    val floats = PrimitiveBlock.Float32(OwnedFloats.copyOf(Array(1.0f, 1.5f, 0.5f)))
    val encodedFloats = zvalue(floatDelta.encodeArray(floats, shape(3L)))
    encodedFloats.block match
      case PrimitiveBlock.Float16(values) =>
        assertEquals(
          values.toArray.toVector.map(bits => HalfFloat.fromBits(bits & 0xffff)),
          Vector(1.0f, 0.5f, -1.0f)
        )
      case _ => fail("expected float16 block")

    val empty = PrimitiveBlock.Int16(OwnedShorts.copyOf(Array.emptyShortArray))
    assertEquals(
      zvalue(deltaCodecForEmpty.encodeArray(empty, shape(0L))).block.elementCount,
      0
    )

  test("delta composes with the synchronous array runtime"):
    val delta = codec(BuiltInDataTypes.int16, "<i2", Some("|i1"))
    val shapeValue = shape(4L)
    val original = PrimitiveBlock.Int16(
      OwnedShorts.copyOf(Array[Short](100, 102, 98, 99))
    )
    val encoded = zvalue(
      SyncCodecRuntime.core.encode(
        original,
        BuiltInDataTypes.int16,
        shapeValue,
        program(delta),
        ByteCount.unsafe(1024L)
      )
    )
    assertEquals(encoded.toArray.toVector, Vector[Byte](100, 2, -4, 1))
    assertEquals(
      int16(
        zvalue(
          SyncCodecRuntime.core.decode(
            encoded,
            program(delta),
            BuiltInDataTypes.int16,
            shapeValue,
            DecodeLimits.default
          )
        )
      ),
      int16(original)
    )

  test("delta composes with the asynchronous array runtime"):
    val delta = codec(BuiltInDataTypes.int16, "<i2", Some("|i1"))
    val shapeValue = shape(4L)
    val original = PrimitiveBlock.Int16(
      OwnedShorts.copyOf(Array[Short](100, 102, 98, 99))
    )
    val codecProgram = program(delta)
    AsyncCodecRuntime.core
      .encode(
        original,
        BuiltInDataTypes.int16,
        shapeValue,
        codecProgram,
        ByteCount.unsafe(1024L)
      )
      .flatMap:
        case Left(error)    => Future.failed(new AssertionError(error.message))
        case Right(encoded) =>
          AsyncCodecRuntime.core
            .decode(
              encoded,
              codecProgram,
              BuiltInDataTypes.int16,
              shapeValue,
              DecodeLimits.default
            )
            .map:
              case Left(error)  => fail(error.message)
              case Right(found) => assertEquals(int16(found), int16(original))

  test("delta refuses unsupported families and mismatched source dtypes"):
    assert(
      DeltaCodec
        .fromConfiguration(
          JsonObject.unsafe(
            Vector(
              "dtype" -> JsonValue.Str("<c8"),
              "astype" -> JsonValue.Str("<c8")
            )
          ),
          BuiltInDataTypes.complex64
        )
        .isLeft
    )
    assert(
      DeltaCodec
        .fromConfiguration(
          JsonObject.unsafe(Vector("dtype" -> JsonValue.Str("<i4"))),
          BuiltInDataTypes.int16
        )
        .isLeft
    )

  private val deltaCodecForEmpty: DeltaCodec = codec(BuiltInDataTypes.int16, "<i2")
