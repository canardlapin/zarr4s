package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

class AsyncWriterSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def descriptor(metadata: String): ArrayDescriptor =
    ZarrMetadata.parse(metadata) match
      case Right(ZarrNodeMetadata.Array(array)) => zvalue(ArrayDescriptor.compile(array))
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)

  private val direct = descriptor(
    """{"zarr_format":3,"node_type":"array","shape":[2,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"crc32c"}],"attributes":{},"storage_transformers":[]}"""
  )

  private val directProvider = new AsyncChunkProvider:
    def chunk(
        coordinate: ChunkCoordinate,
        storedShape: Shape
    )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]] =
      val values =
        if coordinate.axis(1) == 0L then Array[Short](1, 2, 5, 6)
        else Array[Short](3, 4, 7, 8)
      Future.successful(
        Right(
          ChunkPayload.Values(
            PrimitiveBlock.Int16(OwnedShorts.copyOf(values))
          )
        )
      )

  private def completed(outcome: WriteOutcome): WriteReceipt = outcome match
    case WriteOutcome.Complete(receipt)    => receipt
    case WriteOutcome.Incomplete(_, error) => fail(error.message)

  test("async direct creation round trips on both runtimes"):
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(store, direct, directProvider)
      .map: outcome =>
        val receipt = completed(outcome)
        assertEquals(receipt.visitedChunks, 2L)
        assertEquals(receipt.encodedChunks, 2L)
        assertEquals(store.writeTrace.map(_.key.value), Vector("c/0/0", "c/0/1", "zarr.json"))
        val sync = zvalue(MemoryStore(store.snapshot))
        val opened = zvalue(SyncZarr.openArray(sync))
        val region = zvalue(
          Region.within(
            direct.shape,
            zvalue(Coordinate(0L, 0L)),
            direct.shape
          )
        )
        val values = zvalue(opened.readRegion(region)).block match
          case PrimitiveBlock.Int16(found) => found.toArray.toVector
          case _                           => fail("expected int16")
        assertEquals(values, (1 to 8).map(_.toShort).toVector)

  test("async v2 creation publishes v2 metadata and chunk keys"):
    val found = descriptor(
      """{"zarr_format":3,"node_type":"array","shape":[2,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"shuffle","configuration":{"elementsize":2}}],"attributes":{},"storage_transformers":[]}"""
    )
    val provider = new AsyncChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]] =
        val payload = coordinate.toVector match
          case Vector(0L, 0L) =>
            ChunkPayload.Values(PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, 2, 5, 6))))
          case Vector(0L, 1L) =>
            ChunkPayload.Values(PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](3, 4, 7, 8))))
          case _ => ChunkPayload.Fill
        Future.successful(Right(payload))
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(store, found, provider, format = ZarrFormat.V2)
      .map: outcome =>
        val receipt = completed(outcome)
        assertEquals(store.writeTrace.map(_.key.value), Vector(".zattrs", "0/0", "0/1", ".zarray"))
        assertEquals(receipt.metadataObjects.map(_.key.value), Vector(".zattrs"))
        assert(!store.snapshot.contains("zarr.json"))
        val opened = zvalue(SyncZarr.openArray(zvalue(MemoryStore(store.snapshot))))
        val region = zvalue(Region.within(found.shape, zvalue(Coordinate(0L, 0L)), found.shape))
        val values = zvalue(opened.readRegion(region)).block match
          case PrimitiveBlock.Int16(found) => found.toArray.toVector
          case _                           => fail("expected int16")
        assertEquals(values, Vector[Short](1, 2, 3, 4, 5, 6, 7, 8))

  test("async indexed sharding reproduces the synchronous object"):
    val found = descriptor(ZarrBinaryFixtures.shardedEndMetadata)
    val provider = new AsyncChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]] =
        val payload = coordinate.toVector match
          case Vector(0L, 0L) =>
            ChunkPayload.Values(
              PrimitiveBlock.Int16(
                OwnedShorts.copyOf(Array[Short](1, 2, 3, 4))
              )
            )
          case Vector(1L, 1L) =>
            ChunkPayload.Values(
              PrimitiveBlock.Int16(
                OwnedShorts.copyOf(Array[Short](13, 14, 15, 16))
              )
            )
          case _ => ChunkPayload.Fill
        Future.successful(Right(payload))
    val store = zvalue(AsyncMemoryStore(Map.empty))
    AsyncZarrWriter
      .create(store, found, provider)
      .map: outcome =>
        val receipt = completed(outcome)
        assertEquals(store.snapshot("c/0/0"), ZarrBinaryFixtures.shardedEndObject)
        assertEquals(receipt.visitedChunks, 4L)
        assertEquals(receipt.encodedChunks, 2L)
        assertEquals(receipt.omittedFillChunks, 2L)

  test("async creation waits for each object before requesting the next chunk"):
    val firstWrite = Promise[Unit]()
    val release = Promise[Either[StoreError, Unit]]()
    var providerCalls = 0
    val provider = new AsyncChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]] =
        providerCalls += 1
        directProvider.chunk(coordinate, storedShape)
    var writeCalls = 0
    val store = new AsyncObjectWriter:
      def create(
          key: StoreKey,
          bytes: OwnedBytes
      ): Future[Either[StoreError, Unit]] =
        writeCalls += 1
        if writeCalls == 1 then
          firstWrite.success(())
          release.future
        else Future.successful(Right(()))

    val writing = AsyncZarrWriter.create(store, direct, provider)
    firstWrite.future.flatMap: _ =>
      assertEquals(providerCalls, 1)
      assertEquals(writeCalls, 1)
      release.success(Right(()))
      writing.map: outcome =>
        completed(outcome)
        assertEquals(providerCalls, 2)
        assertEquals(writeCalls, 3)

  test("async conflicts preserve created-object progress and omit metadata"):
    val conflictKey = zvalue(StoreKey.from("c/0/1"))
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          conflictKey.value -> OwnedBytes.copyOf(Array[Byte](99))
        )
      )
    )
    AsyncZarrWriter
      .create(store, direct, directProvider)
      .map:
        case WriteOutcome.Complete(_) => fail("conflicting creation must be incomplete")
        case WriteOutcome.Incomplete(progress, error) =>
          assertEquals(error, ZarrError.StoreFailure(StoreError.AlreadyExists(conflictKey)))
          assertEquals(progress.objects.map(_.key.value), Vector("c/0/0"))
          assertEquals(progress.visitedChunks, 2L)
          assert(!store.snapshot.contains("zarr.json"))
