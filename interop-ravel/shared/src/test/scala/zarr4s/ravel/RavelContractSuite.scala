package zarr4s.ravel

import _root_.ravel.{
  AnyRank,
  NDArray,
  Shape as RavelShape,
  UInt8 as RavelUInt8,
  UInt16 as RavelUInt16
}
import _root_.zarr4s.*
import scala.compiletime.testing.typeCheckErrors
import scala.concurrent.ExecutionContext

class RavelContractSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def roundTrip[D <: DType & Singleton, A, R <: AnyRank](
      dtype: D,
      array: NDArray[A, R]
  )(using
      mapping: RavelElement[D],
      elementType: A =:= RavelValue[D]
  ): Unit =
    val source = rvalue(
      RavelArraySource.fromCanonical[D, A, R](dtype, array)(using mapping, elementType)
    )
    val typedArray = elementType.substituteCo[[Element] =>> NDArray[Element, R]](array)
    val store = zvalue(MemoryStore.empty)
    val created = rvalue(
      RavelZarr.createAndOpenArray(
        store,
        zvalue(ArraySpec(dtype, source.shape, source.shape)),
        source
      )
    )
    val result = rvalue(zvalue(created.opened).readAllNDArray())
    assert(result.data.sameElementsBits(typedArray), s"${dtype.name} bitwise round trip")

  test("Float32 canonical source writes and reads an owned NDArray with its receipt"):
    val array = NDArray.fromSeq(RavelShape(2, 3), Seq(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f))
    val source = rvalue(RavelArraySource.fromCanonical(DType.Float32, array))
    val shape = zvalue(Shape(2L, 3L))
    val spec = zvalue(ArraySpec(DType.Float32, shape, zvalue(Shape(1L, 2L))))
    val store = zvalue(MemoryStore.empty)
    val created = rvalue(RavelZarr.createAndOpenArray(store, spec, source))
    val opened = zvalue(created.opened)
    val result = rvalue(opened.readAllNDArray())

    assertEquals(result.data.elementsIterator.toVector, array.elementsIterator.toVector)
    assertEquals(result.data.shape.toString, "(2, 3)")
    assertEquals(result.receipt.requestedElements, 6L)
    assert(created.outcome.toEither.isRight)

  test("UInt16 preserves maximum raw-bit values without widening"):
    val values = Seq(0, 1, 32768, 65535).map(RavelUInt16.unsafe)
    val array = NDArray.fromSeq(RavelShape(2, 2), values)
    val source = rvalue(RavelArraySource.fromCanonical(DType.UInt16, array))
    val shape = zvalue(Shape(2L, 2L))
    val store = zvalue(MemoryStore.empty)
    val created = rvalue(
      RavelZarr.createAndOpenArray(
        store,
        zvalue(ArraySpec(DType.UInt16, shape, zvalue(Shape(2L, 1L)))),
        source
      )
    )
    val result = rvalue(zvalue(created.opened).readAllNDArray())
    assertEquals(result.data.elementsIterator.map(_.toInt).toVector, Vector(0, 1, 32768, 65535))

  test("all exact dtype mappings round trip bitwise"):
    roundTrip(DType.Bool, NDArray.fromSeq(RavelShape(2, 2), Seq(false, true, true, false)))
    roundTrip(
      DType.Int8,
      NDArray.fromSeq(RavelShape(3), Seq(Byte.MinValue, 0.toByte, Byte.MaxValue))
    )
    roundTrip(
      DType.UInt8,
      NDArray.fromSeq(RavelShape(3), Seq(0, 128, 255).map(RavelUInt8.unsafe))
    )
    roundTrip(
      DType.Int16,
      NDArray.fromSeq(RavelShape(3), Seq(Short.MinValue, 0.toShort, Short.MaxValue))
    )
    roundTrip(
      DType.UInt16,
      NDArray.fromSeq(RavelShape(3), Seq(0, 32768, 65535).map(RavelUInt16.unsafe))
    )
    roundTrip(DType.Int32, NDArray.fromSeq(RavelShape(3), Seq(Int.MinValue, 0, Int.MaxValue)))
    roundTrip(DType.Int64, NDArray.fromSeq(RavelShape(3), Seq(Long.MinValue, 0L, Long.MaxValue)))
    roundTrip(
      DType.Float32,
      NDArray.fromSeq(
        RavelShape(4),
        Seq(
          -0.0f,
          Float.PositiveInfinity,
          Float.NegativeInfinity,
          java.lang.Float.intBitsToFloat(0x7fc01234)
        )
      )
    )
    roundTrip(
      DType.Float64,
      NDArray.fromSeq(
        RavelShape(4),
        Seq(
          -0.0,
          Double.PositiveInfinity,
          Double.NegativeInfinity,
          java.lang.Double.longBitsToDouble(0x7ff8000000001234L)
        )
      )
    )

  test("non-canonical views are rejected unless copying is explicit"):
    val view = NDArray.fromSeq(RavelShape(2, 3), 1 to 6).transpose
    val rejected = RavelArraySource.fromCanonical(DType.Int32, view)
    assert(rejected.left.exists(_.isInstanceOf[RavelInteropError.NonCanonicalInput]), rejected)

    val copied = rvalue(RavelArraySource.copyOf(DType.Int32, view))
    assertEquals(copied.shape, zvalue(Shape(3L, 2L)))

  test("scalar empty and rank-five Ravel shapes remain dynamic and checked"):
    val scalar = NDArray.scalar(42.0f)
    val empty = NDArray.zeros[Float, _root_.ravel.Rank[2]](RavelShape(0, 3))
    val rankFiveShape = rvalue(
      RavelShapeBridge.fromZarr(zvalue(Shape(1L, 1L, 1L, 1L, 2L)))
    )
    val rankFive = NDArray.fill(rankFiveShape, 7.0f)

    assertEquals(
      rvalue(RavelArraySource.fromCanonical(DType.Float32, scalar)).shape,
      zvalue(Shape())
    )
    assertEquals(
      rvalue(RavelArraySource.fromCanonical(DType.Float32, empty)).shape,
      zvalue(Shape(0L, 3L))
    )
    assertEquals(rankFive.rank, 5)

  test("Long dimensions and total products fail before Ravel allocation"):
    assertEquals(
      RavelShapeBridge.fromZarr(zvalue(Shape(Int.MaxValue.toLong + 1L, 0L))),
      Left(RavelInteropError.ShapeNotRepresentable(0, Int.MaxValue.toLong + 1L))
    )
    assertEquals(
      RavelShapeBridge.fromZarr(zvalue(Shape(46341L, 46341L))),
      Left(RavelInteropError.ElementCountNotRepresentable(2147488281L))
    )

  test("unsupported exact mappings fail at compile time"):
    val errors = typeCheckErrors(
      """
        import zarr4s.ravel.*
        import zarr4s.DType
        summon[RavelElement[DType.Float16.type]]
        summon[RavelElement[DType.UInt32.type]]
        summon[RavelElement[DType.UInt64.type]]
        summon[RavelElement[DType.Complex64.type]]
        summon[RavelElement[DType.Complex128.type]]
      """
    )
    assert(errors.nonEmpty)

  test("runtime dtype refinement rejects unsupported representations explicitly"):
    assertEquals(
      RavelElement.exact(DType.UInt32),
      Left(RavelInteropError.UnsupportedDType("uint32"))
    )
    assertEquals(rvalue(RavelElement.exact(DType.Float64)).dtype.name, DType.Float64.name)

  test("async creation and read preserve the same public evidence"):
    val array = NDArray.fromSeq(RavelShape(2), Seq(3.0f, 4.0f))
    val source = rvalue(RavelArraySource.fromCanonical(DType.Float32, array))
    val shape = zvalue(Shape(2L))
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncRavelZarr
      .createAndOpenArray(store, zvalue(ArraySpec(DType.Float32, shape, shape)), source)
      .flatMap:
        case Left(error)    => fail(error.message)
        case Right(created) =>
          created.opened match
            case Left(error)   => fail(error.message)
            case Right(opened) => opened.readAllNDArrayAsync()
      .map:
        case Left(error)   => fail(error.message)
        case Right(result) =>
          assertEquals(result.data.elementsIterator.toVector, Vector(3.0f, 4.0f))
          assertEquals(result.receipt.requestedElements, 2L)
