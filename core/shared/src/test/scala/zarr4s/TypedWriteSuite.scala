package zarr4s

import scala.concurrent.ExecutionContext

class TypedWriteSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def spec(
      shape: Shape,
      chunks: Shape,
      format: ZarrFormat = ZarrFormat.V3
  ): ArraySpec[DType.Int16.type] =
    value(ArraySpec(DType.Int16, shape, chunks)).asFormat(format)

  private def dense(shape: Shape, values: Array[Short]): DenseArray[DType.Int16.type] =
    value(DenseArray.copyOf(DType.Int16, shape, values))

  test("createAndOpenArray joins spec, dense data, writer, open, and typed read"):
    val shape = value(Shape(2L, 3L))
    val chunks = value(Shape(2L, 3L))
    val store = value(MemoryStore.empty)
    val created = value(
      SyncZarr.createAndOpenArray(
        store,
        spec(shape, chunks),
        dense(shape, Array[Short](1, 2, 3, 4, 5, 6))
      )
    )
    val opened = value(created.opened)
    val result = value(opened.readAll())
    assertEquals(result.data.toArray.toVector, Vector[Short](1, 2, 3, 4, 5, 6))
    assert(created.outcome.toEither.isRight)
    assertEquals(created.descriptor, created.write.descriptor)

  test("writer-only creation retains the descriptor and complete outcome"):
    val shape = value(Shape(2L))
    val descriptorStore = new ObjectWriter:
      def create(_key: StoreKey, _bytes: OwnedBytes): Either[StoreError, Unit] = Right(())
    val result = value(
      SyncZarr.createArray(
        descriptorStore,
        spec(shape, shape),
        dense(shape, Array[Short](8, 9))
      )
    )
    assert(result.outcome.toEither.isRight)
    assertEquals(result.descriptor.shape, shape)

  test("fill-only creation omits physical chunks and reads typed fill values"):
    val shape = value(Shape(2L, 2L))
    val chunks = value(Shape(1L, 1L))
    val store = value(MemoryStore.empty)
    val filled = spec(shape, chunks).withFill(7.toShort)
    val result = value(SyncZarr.createFillArray(store, filled))
    val opened = value(SyncZarr.openTypedArray(store, DType.Int16))
    val read = value(opened.readAll())
    assertEquals(read.data.toArray.toVector, Vector[Short](7, 7, 7, 7))
    result.outcome match
      case WriteOutcome.Complete(receipt)    => assertEquals(receipt.omittedFillChunks, 4L)
      case WriteOutcome.Incomplete(_, error) => fail(error.message)

  test("explicit typed providers and sharding use the same facade contract"):
    val shape = value(Shape(4L, 4L))
    val outer = value(Shape(4L, 4L))
    val inner = value(Shape(2L, 2L))
    val arraySpec = spec(shape, outer)
    val descriptor = value(ArrayDescriptor.sharded(arraySpec, ShardingSpec.indexed(inner)))
    val data = dense(shape, (1 to 16).map(_.toShort).toArray)
    val untyped = value(ChunkProvider.fromDense(descriptor, data))
    val typed = TypedChunkProvider.from(DType.Int16, untyped)
    val store = value(MemoryStore.empty)
    val result = value(
      SyncZarr.createArrayFromProvider(
        store,
        arraySpec,
        typed,
        sharding = Some(ShardingSpec.indexed(inner))
      )
    )
    assert(result.outcome.toEither.isRight)
    val read = value(SyncZarr.openTypedArray(store, DType.Int16)).readAll() match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assertEquals(read.data.toArray.toVector, (1 to 16).map(_.toShort).toVector)

  test("v2 and nested paths remain explicit"):
    val shape = value(Shape(2L))
    val path = value(ZarrPath("nested/example"))
    val store = value(MemoryStore.empty)
    val result = value(
      SyncZarr.createArray(
        store,
        spec(shape, shape, ZarrFormat.V2),
        dense(shape, Array[Short](3, 4)),
        path = path
      )
    )
    assert(result.outcome.toEither.isRight)
    val opened = value(SyncZarr.openTypedArray(store, DType.Int16, path = path))
    assertEquals(value(opened.readAll()).data.toArray.toVector, Vector[Short](3, 4))

  test("incomplete outcomes preserve progress and conflict semantics"):
    val shape = value(Shape(2L, 2L))
    val chunks = value(Shape(1L, 1L))
    val store = value(MemoryStore.empty)
    val first = value(
      SyncZarr.createArray(
        store,
        spec(shape, chunks),
        dense(shape, Array[Short](1, 2, 3, 4))
      )
    )
    assert(first.outcome.toEither.isRight)

    val conflict = value(
      SyncZarr.createArray(
        store,
        spec(shape, chunks),
        dense(shape, Array[Short](5, 6, 7, 8))
      )
    )
    conflict.outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.StoreFailure(StoreError.AlreadyExists(_))) =>
        assertEquals(progress.createdObjects, 0)
      case other => fail(s"expected create-only conflict, found $other")

    val limitedStore = value(MemoryStore.empty)
    val limited = value(
      SyncZarr.createArray(
        limitedStore,
        spec(shape, chunks),
        dense(shape, Array[Short](1, 2, 3, 4)),
        limits = WriterLimits(maxChunks = 1L)
      )
    )
    limited.outcome match
      case WriteOutcome.Incomplete(progress, ZarrError.ResourceLimit("visited chunks", 1L, 2L)) =>
        assertEquals(progress.visitedChunks, 1L)
        assertEquals(limited.descriptor.shape, shape)
      case other => fail(s"expected typed limit failure, found $other")

  test("async facade produces the same object map and typed receipt"):
    val shape = value(Shape(2L, 3L))
    val chunks = value(Shape(2L, 3L))
    val source = dense(shape, Array[Short](1, 2, 3, 4, 5, 6))
    val syncStore = value(MemoryStore.empty)
    val syncResult = value(SyncZarr.createArray(syncStore, spec(shape, chunks), source))
    val asyncStore = value(AsyncMemoryStore(Map.empty))
    AsyncZarr
      .createArray(asyncStore, spec(shape, chunks), source)
      .map:
        case Left(error)        => fail(error.message)
        case Right(asyncResult) =>
          assert(asyncResult.outcome.toEither.isRight)
          assertEquals(asyncStore.snapshot, syncStore.snapshot)
          assertEquals(
            asyncResult.receipt.map(_.totalObjects),
            syncResult.receipt.map(_.totalObjects)
          )

  test("async create-and-open returns a typed handle only after completion"):
    val shape = value(Shape(2L))
    val store = value(AsyncMemoryStore(Map.empty))
    AsyncZarr
      .createAndOpenArray(store, spec(shape, shape), dense(shape, Array[Short](11, 12)))
      .flatMap:
        case Left(error)    => fail(error.message)
        case Right(created) =>
          created.opened match
            case Left(error)   => fail(error.message)
            case Right(opened) =>
              opened
                .readAll()
                .map:
                  case Left(error)   => fail(error.message)
                  case Right(result) =>
                    assertEquals(result.data.toArray.toVector, Vector[Short](11, 12))
