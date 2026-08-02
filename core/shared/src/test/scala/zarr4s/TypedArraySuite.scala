package zarr4s

import scala.compiletime.testing.typeCheckErrors

class TypedArraySuite extends munit.FunSuite:
  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("typed dense values preserve the dtype witness and defensive ownership"):
    val shape = value(Shape(2L, 3L))
    val source = Array[Short](1, 2, 3, 4, 5, 6)
    val found = value(DenseArray.copyOf(DType.Int16, shape, source))
    val typed: DenseArray[DType.Int16.type] = found

    source(0) = 99
    assertEquals(typed.dtype, DType.Int16)
    assertEquals(typed.shape, shape)
    assertEquals(typed.toArray.toVector, Vector[Short](1, 2, 3, 4, 5, 6))
    assertEquals(typed(5), 6.toShort)

  test("adopt transfers an owned primitive array without copying"):
    val shape = value(Shape(2L))
    val source = Array[Short](7, 8)
    val adopted = value(DenseArray.adopt(DType.Int16, shape, source))
    assert(adopted.values eq source)
    source(0) = 9
    assertEquals(adopted(0), 9.toShort)
    val exposed = adopted.toArray
    exposed(0) = 11
    assertEquals(adopted(0), 9.toShort)

  test("typed dense construction validates scalar, empty, and arbitrary-rank counts"):
    val scalar = value(DenseArray.copyOf(DType.Int32, value(Shape()), Array(7)))
    assertEquals(scalar.shape.rank.toInt, 0)
    assertEquals(scalar.toArray.toVector, Vector(7))

    val emptyShape = value(Shape(2L, 0L, 4L))
    val empty = value(DenseArray.copyOf(DType.Float64, emptyShape, Array.emptyDoubleArray))
    assertEquals(empty.length, 0)

    val rankFiveShape = value(Shape(1L, 1L, 1L, 2L, 3L))
    val rankFive =
      value(DenseArray.copyOf(DType.UInt16, rankFiveShape, Array[Short](1, 2, 3, 4, 5, 6)))
    assertEquals(rankFive.shape.rank.toInt, 5)
    assertEquals(
      DenseArray.copyOf(DType.Int16, value(Shape(2L, 2L)), Array[Short](1, 2)).map(_.length),
      Left(ZarrError.InvalidShape("dense value length 2 does not match shape element count 4"))
    )

  test("all fixed-width dtype witnesses map to executable primitive blocks"):
    val shape = value(Shape(2L))
    val cases = Vector(
      value(DenseArray.copyOf(DType.Bool, shape, Array(true, false))).toArray.toVector,
      value(DenseArray.copyOf(DType.Int8, shape, Array[Byte](1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.UInt8, shape, Array[Byte](1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.UInt16, shape, Array[Short](1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.Int32, shape, Array(1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.UInt32, shape, Array(1, 2))).toArray.toVector,
      value(DenseArray.copyOf(DType.Int64, shape, Array(1L, 2L))).toArray.toVector,
      value(DenseArray.copyOf(DType.UInt64, shape, Array(1L, 2L))).toArray.toVector,
      value(DenseArray.copyOf(DType.Float16, shape, Array(1.5f, 2.5f))).toArray.toVector,
      value(DenseArray.copyOf(DType.Float32, shape, Array(1.5f, 2.5f))).toArray.toVector,
      value(DenseArray.copyOf(DType.Float64, shape, Array(1.5d, 2.5d))).toArray.toVector,
      value(
        DenseArray.copyOf(
          DType.Complex64,
          shape,
          Array(Complex64Value(1.0f, 2.0f), Complex64Value(3.0f, 4.0f))
        )
      ).toArray.toVector,
      value(
        DenseArray.copyOf(
          DType.Complex128,
          shape,
          Array(Complex128Value(1.0d, 2.0d), Complex128Value(3.0d, 4.0d))
        )
      ).toArray.toVector
    )
    assertEquals(cases.length, DType.all.length)

  test("floating special values and complex components round-trip through typed storage"):
    val shape = value(Shape(3L))
    val floating = value(
      DenseArray.copyOf(
        DType.Float64,
        shape,
        Array(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)
      )
    )
    val floatingValues = floating.toArray
    assert(floatingValues(0).isNaN)
    assertEquals(floatingValues(1), Double.PositiveInfinity)
    assertEquals(floatingValues(2), Double.NegativeInfinity)

    val complex = value(
      DenseArray.copyOf(
        DType.Complex64,
        value(Shape(2L)),
        Array(Complex64Value(1.0f, -0.0f), Complex64Value(Float.NaN, Float.PositiveInfinity))
      )
    )
    assertEquals(complex(0), Complex64Value(1.0f, -0.0f))
    assert(complex(1).real.isNaN)
    assertEquals(complex(1).imaginary, Float.PositiveInfinity)

  test("array specs validate rank and expose specification-level defaults"):
    val shape = value(Shape(2L, 3L))
    val spec = value(ArraySpec(DType.Int16, shape, value(Shape(2L, 3L))))
    assertEquals(spec.dtype, DType.Int16)
    assertEquals(spec.format, ZarrFormat.V3)
    assertEquals(spec.fillValue, None)
    assertEquals(spec.dimensionNames, None)
    assertEquals(spec.attributes, JsonObject.empty)

    val named = value(spec.withDimensionNames(Vector(Some("y"), Some("x"))))
    assertEquals(named.dimensionNames, Some(Vector(Some("y"), Some("x"))))
    assert(
      spec
        .withDimensionNames(Vector(Some("only")))
        .left
        .exists:
          case ZarrError.RankMismatch(2, 1, "dimension names") => true
          case _                                               => false
    )

  test("memory store exposes an empty checked constructor"):
    val store = value(MemoryStore.empty)
    assertEquals(store.snapshot, Map.empty)

  test("the dtype witness rejects assigning an Int16 dense value as Float32"):
    val errors = typeCheckErrors(
      """val wrong: zarr4s.DenseArray[zarr4s.DType.Float32.type] =
        zarr4s.DenseArray
          .copyOf(zarr4s.DType.Int16, zarr4s.Shape(1L).toOption.get, Array[Short](1))
          .toOption
          .get"""
    )
    assert(errors.nonEmpty)
