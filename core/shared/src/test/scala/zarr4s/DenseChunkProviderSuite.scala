package zarr4s

import scala.concurrent.ExecutionContext

class DenseChunkProviderSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def int16Values(payload: ChunkPayload): Vector[Short] = payload match
    case ChunkPayload.Values(PrimitiveBlock.Int16(values)) => values.toArray.toVector
    case ChunkPayload.Values(found) => fail(s"expected int16 block, found $found")
    case ChunkPayload.Fill          => fail("expected values, found fill")

  private def descriptor(
      shape: Shape,
      chunkShape: Shape,
      dtype: DType = DType.Int16
  ): ArrayDescriptor =
    val spec = value(ArraySpec(dtype, shape, chunkShape))
    value(ArrayDescriptor.direct(spec))

  private def coordinate(values: Long*): ChunkCoordinate =
    ChunkCoordinate.unsafe(values.toArray)

  test("dense provider emits C-order values and fill-pads border chunks"):
    val shape = value(Shape(3L, 5L))
    val chunks = value(Shape(2L, 3L))
    val found = descriptor(shape, chunks)
    val data = value(
      DenseArray.copyOf(
        DType.Int16,
        shape,
        Array[Short](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
      )
    )
    val provider = value(ChunkProvider.fromDense(found, data))

    val first = value(provider.chunk(coordinate(0L, 0L), chunks))
    assertEquals(int16Values(first), Vector[Short](1, 2, 3, 6, 7, 8))

    val border = value(provider.chunk(coordinate(1L, 1L), chunks))
    assertEquals(int16Values(border), Vector[Short](14, 15, 0, 0, 0, 0))

  test("dense provider handles scalar and sharded global inner coordinates"):
    val scalarShape = value(Shape())
    val scalar = descriptor(scalarShape, scalarShape)
    val scalarData = value(DenseArray.copyOf(DType.Int16, scalarShape, Array[Short](42)))
    val scalarProvider = value(ChunkProvider.fromDense(scalar, scalarData))
    assertEquals(
      int16Values(value(scalarProvider.chunk(coordinate(), scalarShape))),
      Vector[Short](42)
    )

    val shape = value(Shape(8L, 8L))
    val outer = value(Shape(4L, 8L))
    val inner = value(Shape(4L, 4L))
    val spec = value(ArraySpec(DType.Int16, shape, outer))
    val sharded = value(
      ArrayDescriptor.sharded(
        spec,
        ShardingSpec.indexed(
          inner,
          innerCodecs = Vector(ArrayCodecSpec.Bytes.little),
          indexCodecs = Vector(ArrayCodecSpec.Bytes.little, ArrayCodecSpec.Crc32c)
        )
      )
    )
    val values = (1 to 64).map(_.toShort).toArray
    val data = value(DenseArray.copyOf(DType.Int16, shape, values))
    val provider = value(ChunkProvider.fromDense(sharded, data))
    val expected = Vector[Short](37, 38, 39, 40, 45, 46, 47, 48, 53, 54, 55, 56, 61, 62, 63, 64)
    assertEquals(int16Values(value(provider.chunk(coordinate(1L, 1L), inner))), expected)

  test("dense provider rejects dtype, shape, coordinate, and stored-shape mismatches"):
    val shape = value(Shape(2L, 2L))
    val chunks = value(Shape(2L, 2L))
    val found = descriptor(shape, chunks)
    val wrongDType = value(DenseArray.copyOf(DType.Float32, shape, Array(1.0f, 2.0f, 3.0f, 4.0f)))
    assert(
      ChunkProvider
        .fromDense(found, wrongDType)
        .left
        .exists:
          case ZarrError.DTypeMismatch("int16", "float32", "dense chunk provider") => true
          case _                                                                   => false
    )

    val wrongShape =
      value(DenseArray.copyOf(DType.Int16, value(Shape(4L)), Array[Short](1, 2, 3, 4)))
    assert(ChunkProvider.fromDense(found, wrongShape).isLeft)

    val data = value(DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4)))
    val provider = value(ChunkProvider.fromDense(found, data))
    assert(provider.chunk(coordinate(1L, 0L), chunks).isLeft)
    assert(provider.chunk(coordinate(0L, 0L), value(Shape(1L, 2L))).isLeft)

  test("fill provider and async adapter preserve explicit fill semantics"):
    val shape = value(Shape(2L, 3L))
    val found = descriptor(shape, value(Shape(2L, 3L)))
    val provider = ChunkProvider.fill(found)
    assertEquals(
      provider.chunk(coordinate(0L, 0L), found.grid.chunkShape),
      Right(ChunkPayload.Fill)
    )

    val async = AsyncChunkProvider.fromSync(provider)
    val completed = async.chunk(coordinate(0L, 0L), found.grid.chunkShape)(using
      scala.concurrent.ExecutionContext.parasitic
    )
    assertEquals(completed.value.get.get, Right[ZarrError, ChunkPayload](ChunkPayload.Fill))

  test("dense provider preserves boolean and complex primitive carriers"):
    val shape = value(Shape(2L))
    val chunks = value(Shape(2L))
    val boolDescriptor = descriptor(shape, chunks, DType.Bool)
    val boolData = value(DenseArray.copyOf(DType.Bool, shape, Array(true, false)))
    val boolProvider = value(ChunkProvider.fromDense(boolDescriptor, boolData))
    value(boolProvider.chunk(coordinate(0L), chunks)) match
      case ChunkPayload.Values(PrimitiveBlock.Bool(values)) =>
        assertEquals(values.toArray.toVector, Vector(true, false))
      case found => fail(s"expected bool payload, found $found")

    val complexShape = value(Shape(1L))
    val complexDescriptor = descriptor(complexShape, complexShape, DType.Complex64)
    val complexData = value(
      DenseArray.copyOf(
        DType.Complex64,
        complexShape,
        Array(Complex64Value(1.5f, -2.0f))
      )
    )
    val complexProvider = value(ChunkProvider.fromDense(complexDescriptor, complexData))
    value(complexProvider.chunk(coordinate(0L), complexShape)) match
      case ChunkPayload.Values(PrimitiveBlock.Complex64(values)) =>
        assertEquals(values.real(0), 1.5f)
        assertEquals(values.imaginary(0), -2.0f)
      case found => fail(s"expected complex payload, found $found")

  test("empty arrays construct a provider without inventing coordinates"):
    val shape = value(Shape(0L, 3L))
    val chunks = value(Shape(2L, 3L))
    val found = descriptor(shape, chunks)
    val data = value(DenseArray.copyOf(DType.Int16, shape, Array.emptyShortArray))
    val provider = value(ChunkProvider.fromDense(found, data))
    assert(provider.chunk(coordinate(0L, 0L), chunks).isLeft)

  test("sync and async writers preserve provider limits and fill-only omission"):
    val shape = value(Shape(2L, 2L))
    val chunks = value(Shape(1L, 1L))
    val found = descriptor(shape, chunks)
    val data = value(DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4)))
    val provider = value(ChunkProvider.fromDense(found, data))
    val syncStore = value(MemoryStore.empty)
    SyncZarrWriter.create(
      syncStore,
      found,
      provider,
      limits = WriterLimits(maxChunks = 1L)
    ) match
      case WriteOutcome.Incomplete(_, ZarrError.ResourceLimit("visited chunks", 1L, 2L)) => ()
      case other => fail(s"expected a typed sync limit failure, found $other")

    val asyncStore = value(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(
        asyncStore,
        found,
        AsyncChunkProvider.fromSync(provider),
        limits = WriterLimits(maxChunks = 1L)
      )
      .map:
        case WriteOutcome.Incomplete(_, ZarrError.ResourceLimit("visited chunks", 1L, 2L)) =>
          val fillStore = value(MemoryStore.empty)
          val fillOutcome = SyncZarrWriter.create(fillStore, found, ChunkProvider.fill(found))
          assert(fillOutcome.toEither.isRight)
          assertEquals(
            fillOutcome match
              case WriteOutcome.Complete(receipt) => receipt.omittedFillChunks
              case WriteOutcome.Incomplete(_, _)  => -1L,
            4L
          )
        case other => fail(s"expected a typed async limit failure, found $other")
