package zarr4s

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

class JvmTypedFacadeSuite extends munit.FunSuite:
  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def spec(shape: Shape, chunks: Shape): ArraySpec[DType.Int16.type] =
    value(ArraySpec(DType.Int16, shape, chunks))

  private def data(shape: Shape, values: Short*): DenseArray[DType.Int16.type] =
    value(DenseArray.copyOf(DType.Int16, shape, values.toArray))

  test("JVM Path create-and-open publishes atomically and reads typed values"):
    val parent = Files.createTempDirectory("zarr4s-typed-jvm")
    val target = parent.resolve("nested").resolve("array.zarr")
    val shape = value(Shape(2L, 2L))
    val created = value(
      JvmZarr.createAndOpenArray(
        target,
        spec(shape, value(Shape(1L, 2L))),
        data(shape, 1, 2, 3, 4)
      )
    )
    val opened = value(created.opened)
    assertEquals(value(opened.readAll()).data.toArray.toVector, Vector[Short](1, 2, 3, 4))
    assert(Files.exists(target.resolve("zarr.json")))
    assert(created.outcome.toEither.isRight)

  test("JVM Path facade preserves conflicts and incomplete progress"):
    val parent = Files.createTempDirectory("zarr4s-typed-jvm-conflict")
    val target = parent.resolve("array.zarr")
    val shape = value(Shape(2L, 2L))
    val arraySpec = spec(shape, value(Shape(1L, 1L)))
    val source = data(shape, 1, 2, 3, 4)
    val first = value(JvmZarr.createArray(target, arraySpec, source))
    assert(first.outcome.toEither.isRight)
    val conflict = value(JvmZarr.createArray(target, arraySpec, source))
    conflict.outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.WriteFailure(detail)) =>
        assertEquals(progress.createdObjects, 0)
        assert(detail.contains("already exists"))
      case other => fail(s"expected a create-only conflict, found $other")

    val limitedTarget = parent.resolve("limited.zarr")
    val limited = value(
      JvmZarr.createArray(
        limitedTarget,
        arraySpec,
        source,
        limits = WriterLimits(maxChunks = 1L)
      )
    )
    limited.outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.ResourceLimit("visited chunks", 1L, 2L)) =>
        assertEquals(progress.visitedChunks, 1L)
        assert(!Files.exists(limitedTarget))
      case other => fail(s"expected a typed incomplete outcome, found $other")

  test("JVM Path facade retains fill-only omission"):
    val parent = Files.createTempDirectory("zarr4s-typed-jvm-fill")
    val target = parent.resolve("fill.zarr")
    val shape = value(Shape(2L, 2L))
    val filled = spec(shape, value(Shape(1L, 1L))).withFill(9.toShort)
    val result = value(JvmZarr.createFillArray(target, filled))
    result.outcome match
      case WriteOutcome.Complete(receipt)    => assertEquals(receipt.omittedFillChunks, 4L)
      case WriteOutcome.Incomplete(_, error) => fail(error.message)
    val store = JvmFileStore.open(target).fold(fail(_), identity)
    val opened =
      value(SyncZarr.openTypedArray(store, DType.Int16, runtime = JvmCodecRuntime.portable))
    assertEquals(value(opened.readAll()).data.toArray.toVector, Vector[Short](9, 9, 9, 9))

  test("JVM staged failure leaves no temporary publication"):
    val parent = Files.createTempDirectory("zarr4s-typed-jvm-failure")
    val target = parent.resolve("failed.zarr")
    val shape = value(Shape(2L, 2L))
    val arraySpec = spec(shape, value(Shape(1L, 1L)))
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        if coordinate.toVector == Vector(0L, 0L) then
          Right(ChunkPayload.Values(PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1)))))
        else Left(ZarrError.WriteFailure("simulated failure"))
    val result = value(
      JvmZarr.createArrayFromProvider(
        target,
        arraySpec,
        TypedChunkProvider.from(DType.Int16, provider)
      )
    )
    assert(result.outcome.toEither.isLeft)
    assert(!Files.exists(target))
    val entries = Files.list(parent)
    try assertEquals(entries.iterator().asScala.toVector, Vector.empty)
    finally entries.close()
