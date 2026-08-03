package zarr4s.ravel

import _root_.ravel.{AnyRank, NDArray, Shape as RavelShape, UInt8, UInt16}
import _root_.zarr4s.*
import scala.compiletime.testing.typeCheckErrors
import scala.concurrent.{ExecutionContext, Future}

class RavelWriteSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def source[D <: DType & Singleton, A, R <: AnyRank](
      dtype: D,
      array: NDArray[A, R]
  )(using
      mapping: RavelElement[D],
      elementType: A =:= RavelValue[D]
  ): RavelArraySource[D, R] =
    rvalue(RavelArraySource.fromCanonical[D, A, R](dtype, array)(using mapping, elementType))

  private def asyncRoundTrip[D <: DType & Singleton, A, R <: AnyRank](
      dtype: D,
      array: NDArray[A, R]
  )(using
      mapping: RavelElement[D],
      elementType: A =:= RavelValue[D]
  ): Future[Unit] =
    val input = source[D, A, R](dtype, array)(using mapping, elementType)
    val typedArray = elementType.substituteCo[[Element] =>> NDArray[Element, R]](array)
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncRavelZarr
      .createAndOpenArray(store, zvalue(ArraySpec(dtype, input.shape, input.shape)), input)
      .flatMap:
        case Left(error)    => fail(error.message)
        case Right(created) =>
          created.opened match
            case Left(error)   => fail(error.message)
            case Right(opened) => opened.readAllNDArrayAsync()
      .map:
        case Left(error)   => fail(error.message)
        case Right(result) =>
          assert(result.data.sameElementsBits(typedArray), s"async ${dtype.name} round trip")

  test("border chunks retain nominal shape and explicit fill overhang"):
    val array = NDArray.fromSeq(RavelShape(3, 5), 0 until 15)
    val input = source(DType.Int32, array)
    val shape = zvalue(Shape(3L, 5L))
    val chunks = zvalue(Shape(2L, 3L))
    val spec = zvalue(ArraySpec(DType.Int32, shape, chunks)).withFill(-7)
    val descriptor = zvalue(ArrayDescriptor.direct(spec))
    val provider = rvalue(input.typedProvider(descriptor)).underlying
    val payload = zvalue(provider.chunk(ChunkCoordinate.unsafe(Array(1L, 1L)), chunks))

    payload match
      case ChunkPayload.Values(PrimitiveBlock.Int32(values)) =>
        assertEquals(values.toArray.toVector, Vector(13, 14, -7, -7, -7, -7))
      case other => fail(s"expected int32 border block, found $other")

  test("v2 and v3 direct writes retain format path receipts and values"):
    Vector(ZarrFormat.V2, ZarrFormat.V3).foreach: format =>
      val array = NDArray.fromSeq(RavelShape(2, 3), 1 to 6)
      val input = source(DType.Int32, array)
      val spec = zvalue(ArraySpec(DType.Int32, input.shape, zvalue(Shape(1L, 2L)))).asFormat(format)
      val path = zvalue(ZarrPath(s"nested/${format.toString.toLowerCase}"))
      val store = zvalue(MemoryStore.empty)
      val created = rvalue(RavelZarr.createAndOpenArray(store, spec, input, path = path))
      val result = rvalue(zvalue(created.opened).readAllNDArray())

      assertEquals(result.data.elementsIterator.toVector, (1 to 6).toVector)
      assert(created.outcome.toEither.isRight)
      assertEquals(created.descriptor.shape, input.shape)

  test("sharded writes reuse the existing provider codec and publication path"):
    val array = NDArray.fromSeq(RavelShape(4, 4), 1 to 16)
    val input = source(DType.Int32, array)
    val inner = zvalue(Shape(2L, 2L))
    val spec = zvalue(ArraySpec(DType.Int32, input.shape, zvalue(Shape(4L, 4L))))
    val store = zvalue(MemoryStore.empty)
    val created = rvalue(
      RavelZarr.createAndOpenArray(
        store,
        spec,
        input,
        sharding = Some(ShardingSpec.indexed(inner))
      )
    )
    val result = rvalue(zvalue(created.opened).readAllNDArray())

    assertEquals(result.data.elementsIterator.toVector, (1 to 16).toVector)
    assert(created.outcome.toEither.isRight)
    assert(result.receipt.rangeRequests > 0)

  test("create-only conflicts and limits retain incomplete progress"):
    val array = NDArray.fromSeq(RavelShape(2, 2), 1 to 4)
    val input = source(DType.Int32, array)
    val spec = zvalue(ArraySpec(DType.Int32, input.shape, zvalue(Shape(1L, 1L))))
    val store = zvalue(MemoryStore.empty)
    assert(rvalue(RavelZarr.createArray(store, spec, input)).outcome.toEither.isRight)

    rvalue(RavelZarr.createArray(store, spec, input)).outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.StoreFailure(StoreError.AlreadyExists(_))) =>
        assertEquals(progress.createdObjects, 0)
      case other => fail(s"expected create-only conflict, found $other")

    val limited = rvalue(
      RavelZarr.createArray(
        zvalue(MemoryStore.empty),
        spec,
        input,
        limits = WriterLimits(maxChunks = 1L)
      )
    )
    limited.outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.ResourceLimit("visited chunks", 1L, 2L)) =>
        assertEquals(progress.visitedChunks, 1L)
        assertEquals(progress.encodedChunks, 1L)
      case other => fail(s"expected retained writer progress, found $other")

  test("all exact dtypes pass the asynchronous create-and-open path"):
    Future
      .sequence(
        Vector(
          asyncRoundTrip(DType.Bool, NDArray.fromSeq(RavelShape(2), Seq(false, true))),
          asyncRoundTrip(
            DType.Int8,
            NDArray.fromSeq(RavelShape(2), Seq(Byte.MinValue, Byte.MaxValue))
          ),
          asyncRoundTrip(
            DType.UInt8,
            NDArray.fromSeq(RavelShape(2), Seq(0, 255).map(UInt8.unsafe))
          ),
          asyncRoundTrip(
            DType.Int16,
            NDArray.fromSeq(RavelShape(2), Seq(Short.MinValue, Short.MaxValue))
          ),
          asyncRoundTrip(
            DType.UInt16,
            NDArray.fromSeq(RavelShape(2), Seq(0, 65535).map(UInt16.unsafe))
          ),
          asyncRoundTrip(
            DType.Int32,
            NDArray.fromSeq(RavelShape(2), Seq(Int.MinValue, Int.MaxValue))
          ),
          asyncRoundTrip(
            DType.Int64,
            NDArray.fromSeq(RavelShape(2), Seq(Long.MinValue, Long.MaxValue))
          ),
          asyncRoundTrip(DType.Float32, NDArray.fromSeq(RavelShape(2), Seq(-0.0f, Float.NaN))),
          asyncRoundTrip(DType.Float64, NDArray.fromSeq(RavelShape(2), Seq(-0.0, Double.NaN)))
        )
      )
      .map(_ => ())

  test("mutable and borrowed arrays are excluded at compile time"):
    val mutableErrors = typeCheckErrors(
      """
        import ravel.*
        import zarr4s.*
        import zarr4s.ravel.*
        val mutable = MutableNDArray.zeros[Int, Rank[1]](Shape(2))
        RavelArraySource.fromCanonical(DType.Int32, mutable)
      """
    )
    val borrowedErrors = typeCheckErrors(
      """
        import ravel.*
        import zarr4s.*
        import zarr4s.ravel.*
        def rejected(value: BorrowedNDArray[Int, Rank[1]]) =
          RavelArraySource.fromCanonical(DType.Int32, value)
      """
    )
    assert(mutableErrors.nonEmpty)
    assert(borrowedErrors.nonEmpty)
