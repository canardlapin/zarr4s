package zarr4s

import scala.concurrent.ExecutionContext

class TypedReadSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private val directBytesMetadata =
    ZarrBinaryFixtures.directGzipMetadata.replace(
      ",\"codecs\":[{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"},{\"configuration\":{\"level\":1},\"name\":\"gzip\"}]",
      ",\"codecs\":[{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"}]"
    )

  private def direct[D <: DType](
      dtype: D,
      shape: Shape,
      chunkShape: Shape,
      fill: Option[dtype.Element] = None
  ): ArrayDescriptor =
    val spec = value(ArraySpec.withOptions(dtype, shape, chunkShape, fill, None))
    value(ArrayDescriptor.direct(spec))

  private def sharded(
      shape: Shape,
      outerChunkShape: Shape,
      innerChunkShape: Shape
  ): ArrayDescriptor =
    val spec = value(ArraySpec(DType.Int16, shape, outerChunkShape))
    value(ArrayDescriptor.sharded(spec, ShardingSpec.indexed(innerChunkShape)))

  private def write[D <: DType](
      store: MemoryStore,
      descriptor: ArrayDescriptor,
      data: DenseArray[D],
      limits: WriterLimits = WriterLimits()
  ): Unit =
    val provider = value(ChunkProvider.fromDense(descriptor, data))
    SyncZarrWriter.create(store, descriptor, provider, limits = limits).toEither match
      case Right(_)    => ()
      case Left(error) => fail(error.message)

  test("typed readAll and dynamic readAll share values, shape, and receipt"):
    val shape = value(Shape(2L, 3L))
    val chunks = value(Shape(2L, 3L))
    val descriptor = direct(DType.Int16, shape, chunks)
    val data = value(
      DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4, 5, 6))
    )
    val store = value(MemoryStore.empty)
    write(store, descriptor, data)
    val opened = value(SyncZarr.openArray(store))

    val dynamic = value(opened.readAll())
    val typed = value(opened.asTyped(DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)

    assertEquals(dynamic.shape, shape)
    val dynamicValues = dynamic.block match
      case PrimitiveBlock.Int16(values) => values.toArray.toVector
      case _                            => Vector.empty[Short]
    assertEquals(dynamicValues, Vector[Short](1, 2, 3, 4, 5, 6))
    assertEquals(typed.data.toArray.toVector, Vector[Short](1, 2, 3, 4, 5, 6))
    assertEquals(typed.shape, dynamic.shape)
    assertEquals(typed.receipt, dynamic.receipt)

  test("typed partial regions retain owned data and receipt accounting"):
    val shape = value(Shape(3L, 4L))
    val descriptor = direct(DType.Int16, shape, value(Shape(2L, 3L)))
    val data = value(
      DenseArray.copyOf(
        DType.Int16,
        shape,
        (1 to 12).map(_.toShort).toArray
      )
    )
    val store = value(MemoryStore.empty)
    write(store, descriptor, data)
    val typed = value(SyncZarr.openTypedArray(store, DType.Int16))
    val region = value(
      Region.within(
        shape,
        value(Coordinate(1L, 1L)),
        value(Shape(2L, 2L))
      )
    )
    val result = value(typed.readRegion(region))
    assertEquals(result.shape, value(Shape(2L, 2L)))
    assertEquals(result.data.toArray.toVector, Vector[Short](6, 7, 10, 11))
    assert(result.receipt.touchedChunks > 0)

  test("readAll covers scalar, empty, and rank-five arrays without region boilerplate"):
    val scalarShape = value(Shape())
    val scalarDescriptor = direct(DType.Int16, scalarShape, scalarShape)
    val scalarStore = value(MemoryStore.empty)
    write(
      scalarStore,
      scalarDescriptor,
      value(DenseArray.copyOf(DType.Int16, scalarShape, Array[Short](42)))
    )
    val scalar = value(SyncZarr.openTypedArray(scalarStore, DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(scalar.shape, scalarShape)
    assertEquals(scalar.data.toArray.toVector, Vector[Short](42))

    val emptyShape = value(Shape(0L, 3L))
    val emptyDescriptor = direct(DType.Int16, emptyShape, value(Shape(2L, 3L)))
    val emptyStore = value(MemoryStore.empty)
    write(
      emptyStore,
      emptyDescriptor,
      value(DenseArray.copyOf(DType.Int16, emptyShape, Array.emptyShortArray))
    )
    emptyStore.clearTrace()
    val empty = value(SyncZarr.openTypedArray(emptyStore, DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(empty.shape, emptyShape)
    assertEquals(empty.data.toArray.toVector, Vector.empty[Short])
    assertEquals(empty.receipt.objectRequests, 0)

    val rankFiveShape = value(Shape(1L, 1L, 1L, 1L, 2L))
    val rankFiveDescriptor = direct(DType.Int16, rankFiveShape, rankFiveShape)
    val rankFiveStore = value(MemoryStore.empty)
    write(
      rankFiveStore,
      rankFiveDescriptor,
      value(DenseArray.copyOf(DType.Int16, rankFiveShape, Array[Short](7, 8)))
    )
    val rankFive = value(SyncZarr.openTypedArray(rankFiveStore, DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(rankFive.data.toArray.toVector, Vector[Short](7, 8))

  test("typed reads support sharded arrays and fill synthesis"):
    val shape = value(Shape(4L, 4L))
    val descriptor = sharded(shape, value(Shape(4L, 4L)), value(Shape(2L, 2L)))
    val data = value(DenseArray.copyOf(DType.Int16, shape, (1 to 16).map(_.toShort).toArray))
    val store = value(MemoryStore.empty)
    write(store, descriptor, data)
    val result = value(SyncZarr.openTypedArray(store, DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(result.data.toArray.toVector, (1 to 16).map(_.toShort).toVector)

  test("dtype refinement fails before any data object is requested"):
    val store = value(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(directBytesMetadata),
          "c/0/0" -> ZarrBinaryFixtures.directDecodedChunk
        )
      )
    )
    store.clearTrace()
    val result = SyncZarr.openTypedArray(store, DType.Float32)
    assert(
      result.left.exists:
        case ZarrError.DTypeMismatch("float32", "int16", "opened array") => true
        case _                                                           => false
    )
    assertEquals(store.trace, Vector(ObjectRequest.Whole(StoreKey.unsafe("zarr.json"))))

  test("malformed encoded data remains a typed read error"):
    val truncated =
      ZarrBinaryFixtures.directDecodedChunk.slice(
        0,
        ZarrBinaryFixtures.directDecodedChunk.length - 2
      )
    val store = value(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(directBytesMetadata),
          "c/0/0" -> truncated
        )
      )
    )
    val typed = value(SyncZarr.openTypedArray(store, DType.Int16))
    assert(typed.readAll().isLeft)

  test("async typed entry point preserves the sync result and receipt"):
    val shape = value(Shape(2L, 3L))
    val descriptor = direct(DType.Int16, shape, shape)
    val source = value(DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4, 5, 6)))
    val syncStore = value(MemoryStore.empty)
    write(syncStore, descriptor, source)
    val asyncStore = value(AsyncMemoryStore(syncStore.snapshot))
    AsyncZarr
      .openTypedArray(asyncStore, DType.Int16)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) => opened.readAll()
      .map:
        case Right(result) =>
          assertEquals(result.data.toArray.toVector, Vector[Short](1, 2, 3, 4, 5, 6))
          assertEquals(result.receipt.requestedElements, 6L)
        case Left(error) => fail(error.message)
