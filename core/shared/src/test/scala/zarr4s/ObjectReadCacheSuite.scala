package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

class ObjectReadCacheSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def svalue[A](result: Either[StoreError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def key(value: String): StoreKey = zvalue(StoreKey.from(value))

  private def range(offset: Long, length: Long): ByteRange =
    zvalue(ByteRange(offset, length))

  private def count(value: Long): ByteCount = zvalue(ByteCount(value))

  private def namespace(value: String): CacheNamespace =
    zvalue(CacheNamespace.from(value))

  private def bytes(values: Int*): OwnedBytes =
    OwnedBytes.copyOf(values.map(_.toByte).toArray)

  private def text(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  test("sync cache reuses exact, containing, whole, and length reads with defensive ownership"):
    val objectKey = key("object")
    val store = zvalue(
      MemoryStore(
        Map(
          objectKey.value -> bytes(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
        )
      )
    )
    val cache = ObjectReadCache(
      namespace("dataset@sha256:one"),
      CacheLimits(8, count(100L))
    )
    val reader = CachingObjectReader(store, cache)

    val first = svalue(reader.read(objectKey, range(2L, 6L)))
    first.values(0) = 99.toByte
    assertEquals(
      svalue(reader.read(objectKey, range(2L, 6L))).toArray.toVector,
      Vector[Byte](2, 3, 4, 5, 6, 7)
    )
    assertEquals(
      svalue(reader.read(objectKey, range(4L, 2L))).toArray.toVector,
      Vector[Byte](4, 5)
    )
    assertEquals(reader.length(objectKey), Right(10L))
    assertEquals(svalue(reader.readAll(objectKey, count(10L))).length, 10)
    assertEquals(reader.length(objectKey), Right(10L))
    assertEquals(
      reader.readAll(objectKey, count(5L)),
      Left(StoreError.ObjectTooLarge(objectKey, 5L, 10L))
    )

    assertEquals(
      store.trace,
      Vector(
        ObjectRequest.Range(objectKey, range(2L, 6L)),
        ObjectRequest.Length(objectKey),
        ObjectRequest.Whole(objectKey)
      )
    )
    assertEquals(
      cache.stats,
      CacheStats(
        hits = 4L,
        misses = 3L,
        downstreamRequests = 3L,
        fetchedBytes = 16L,
        servedBytes = 24L,
        evictedEntries = 0L,
        evictedBytes = 0L,
        singleFlightJoins = 0L,
        residentEntries = 3,
        residentBytes = 16L
      )
    )

  test("entry and byte budgets enforce deterministic least-recently-used eviction"):
    val store = zvalue(
      MemoryStore(
        Map(
          "a" -> bytes(1, 1),
          "b" -> bytes(2, 2),
          "c" -> bytes(3, 3)
        )
      )
    )
    val cache = ObjectReadCache(namespace("revision-lru"), CacheLimits(2, count(8L)))
    val reader = CachingObjectReader(store, cache)
    svalue(reader.readAll(key("a"), count(8L)))
    svalue(reader.readAll(key("b"), count(8L)))
    svalue(reader.readAll(key("a"), count(8L)))
    svalue(reader.readAll(key("c"), count(8L)))
    assertEquals(cache.stats.residentEntries, 2)
    assertEquals(cache.stats.evictedEntries, 1L)
    assertEquals(cache.stats.evictedBytes, 2L)
    svalue(reader.readAll(key("b"), count(8L)))
    assertEquals(store.trace.length, 4)

    val oversizedStore = zvalue(MemoryStore(Map("large" -> bytes(0, 1, 2, 3))))
    val tiny = ObjectReadCache(namespace("revision-tiny"), CacheLimits(4, count(3L)))
    val tinyReader = CachingObjectReader(oversizedStore, tiny)
    svalue(tinyReader.readAll(key("large"), count(4L)))
    svalue(tinyReader.readAll(key("large"), count(4L)))
    assertEquals(oversizedStore.trace.length, 2)
    assertEquals(tiny.stats.residentEntries, 0)

  test("revision identity is explicit and isolated"):
    assert(CacheNamespace.from("").isLeft)
    assert(CacheNamespace.from(" revision ").isLeft)
    val objectKey = key("same-key")
    val oldStore = zvalue(MemoryStore(Map(objectKey.value -> bytes(1))))
    val newStore = zvalue(MemoryStore(Map(objectKey.value -> bytes(2))))
    val oldReader = CachingObjectReader(
      oldStore,
      ObjectReadCache(namespace("manifest-old"), CacheLimits(2, count(8L)))
    )
    val newReader = CachingObjectReader(
      newStore,
      ObjectReadCache(namespace("manifest-new"), CacheLimits(2, count(8L)))
    )
    assertEquals(svalue(oldReader.readAll(objectKey, count(8L))).toArray.toVector, Vector[Byte](1))
    assertEquals(svalue(newReader.readAll(objectKey, count(8L))).toArray.toVector, Vector[Byte](2))

  test("async identical reads collapse to one request and return independent bytes"):
    val objectKey = key("object")
    val release = Promise[Either[StoreError, OwnedBytes]]()
    var calls = 0
    val store = new AsyncObjectReader:
      def read(
          key: StoreKey,
          range: ByteRange
      ): Future[Either[StoreError, OwnedBytes]] =
        Future.successful(Left(StoreError.NotFound(key)))
      def readAll(
          key: StoreKey,
          maxBytes: ByteCount
      ): Future[Either[StoreError, OwnedBytes]] =
        calls += 1
        release.future
      def length(key: StoreKey): Future[Either[StoreError, Long]] =
        Future.successful(Right(4L))
    val cache = ObjectReadCache(namespace("async-one"), CacheLimits(8, count(64L)))
    val reader = CachingAsyncObjectReader(store, cache)
    val first = reader.readAll(objectKey, count(8L))
    val second = reader.readAll(objectKey, count(8L))
    assertEquals(calls, 1)
    release.success(Right(bytes(1, 2, 3, 4)))
    Future
      .sequence(Vector(first, second))
      .flatMap: results =>
        val firstBytes = svalue(results(0))
        val secondBytes = svalue(results(1))
        firstBytes.values(0) = 99.toByte
        assertEquals(secondBytes.toArray.toVector, Vector[Byte](1, 2, 3, 4))
        reader
          .readAll(objectKey, count(8L))
          .map: third =>
            assertEquals(svalue(third).toArray.toVector, Vector[Byte](1, 2, 3, 4))
            assertEquals(calls, 1)
            assertEquals(cache.stats.singleFlightJoins, 1L)
            assertEquals(cache.stats.downstreamRequests, 1L)
            assertEquals(cache.stats.fetchedBytes, 4L)
            assertEquals(cache.stats.servedBytes, 12L)

  test("async identical ranges and lengths each collapse to one request"):
    val objectKey = key("object")
    val requested = range(2L, 3L)
    val rangeRelease = Promise[Either[StoreError, OwnedBytes]]()
    val lengthRelease = Promise[Either[StoreError, Long]]()
    var rangeCalls = 0
    var lengthCalls = 0
    val store = new AsyncObjectReader:
      def read(
          key: StoreKey,
          range: ByteRange
      ): Future[Either[StoreError, OwnedBytes]] =
        rangeCalls += 1
        rangeRelease.future
      def readAll(
          key: StoreKey,
          maxBytes: ByteCount
      ): Future[Either[StoreError, OwnedBytes]] =
        Future.successful(Left(StoreError.NotFound(key)))
      def length(key: StoreKey): Future[Either[StoreError, Long]] =
        lengthCalls += 1
        lengthRelease.future
    val cache = ObjectReadCache(namespace("async-range-length"), CacheLimits(8, count(64L)))
    val reader = CachingAsyncObjectReader(store, cache)
    val ranges = Vector(reader.read(objectKey, requested), reader.read(objectKey, requested))
    val lengths = Vector(reader.length(objectKey), reader.length(objectKey))
    assertEquals(rangeCalls, 1)
    assertEquals(lengthCalls, 1)
    rangeRelease.success(Right(bytes(2, 3, 4)))
    lengthRelease.success(Right(8L))
    Future
      .sequence(ranges)
      .zip(Future.sequence(lengths))
      .map: (rangeResults, lengthResults) =>
        val first = svalue(rangeResults(0))
        val second = svalue(rangeResults(1))
        first.values(0) = 99.toByte
        assertEquals(second.toArray.toVector, Vector[Byte](2, 3, 4))
        assertEquals(lengthResults, Vector(Right(8L), Right(8L)))
        assertEquals(cache.stats.singleFlightJoins, 2L)
        assertEquals(cache.stats.downstreamRequests, 2L)

  test("async store errors and failed futures are never retained"):
    val objectKey = key("retry")
    var errors = 0
    val storeErrors = new AsyncObjectReader:
      def read(
          key: StoreKey,
          range: ByteRange
      ): Future[Either[StoreError, OwnedBytes]] =
        Future.successful(Left(StoreError.NotFound(key)))
      def readAll(
          key: StoreKey,
          maxBytes: ByteCount
      ): Future[Either[StoreError, OwnedBytes]] =
        errors += 1
        if errors == 1 then
          Future.successful(Left(StoreError.Transport(key, "once", transient = true)))
        else Future.successful(Right(bytes(7)))
      def length(key: StoreKey): Future[Either[StoreError, Long]] =
        Future.successful(Right(1L))
    val errorCache = ObjectReadCache(namespace("retry-errors"), CacheLimits(2, count(8L)))
    val errorReader = CachingAsyncObjectReader(storeErrors, errorCache)
    errorReader
      .readAll(objectKey, count(8L))
      .flatMap: first =>
        assert(first.isLeft)
        errorReader
          .readAll(objectKey, count(8L))
          .flatMap: second =>
            assertEquals(svalue(second).toArray.toVector, Vector[Byte](7))
            assertEquals(errors, 2)

            var failures = 0
            val failedFutures = new AsyncObjectReader:
              def read(
                  key: StoreKey,
                  range: ByteRange
              ): Future[Either[StoreError, OwnedBytes]] =
                Future.successful(Left(StoreError.NotFound(key)))
              def readAll(
                  key: StoreKey,
                  maxBytes: ByteCount
              ): Future[Either[StoreError, OwnedBytes]] =
                failures += 1
                if failures == 1 then Future.failed(new IllegalStateException("once"))
                else Future.successful(Right(bytes(8)))
              def length(key: StoreKey): Future[Either[StoreError, Long]] =
                Future.successful(Right(1L))
            val failedReader = CachingAsyncObjectReader(
              failedFutures,
              ObjectReadCache(namespace("retry-future"), CacheLimits(2, count(8L)))
            )
            failedReader
              .readAll(objectKey, count(8L))
              .transformWith:
                case scala.util.Failure(_) =>
                  failedReader
                    .readAll(objectKey, count(8L))
                    .map: retried =>
                      assertEquals(svalue(retried).toArray.toVector, Vector[Byte](8))
                      assertEquals(failures, 2)
                case scala.util.Success(_) => fail("first future must fail")

  test("portable AsyncZarr re-reads direct regions, factored selections, and shards from cache"):
    val directMetadata =
      """{"zarr_format":3,"node_type":"array","shape":[2,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""
    val left = ZarrBinaryFixtures.hex("0100020005000600")
    val right = ZarrBinaryFixtures.hex("0300040007000800")

    def directStore(): AsyncMemoryStore = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> text(directMetadata),
          "c/0/0" -> left,
          "c/0/1" -> right
        )
      )
    )

    def repeatedDirectRegion(): Future[Unit] =
      val store = directStore()
      val reader = CachingAsyncObjectReader(
        store,
        ObjectReadCache(namespace("direct-region"), CacheLimits(16, count(4096L)))
      )
      AsyncZarr
        .openArray(reader)
        .flatMap:
          case Left(error)   => fail(error.message)
          case Right(opened) =>
            val region = zvalue(
              Region.within(
                opened.descriptor.shape,
                zvalue(Coordinate(0L, 0L)),
                opened.descriptor.shape
              )
            )
            store.clearTrace()
            opened
              .readRegion(region)
              .flatMap:
                case Left(error)  => fail(error.message)
                case Right(first) =>
                  val requests = store.trace.length
                  assertEquals(shorts(first), (1 to 8).map(_.toShort).toVector)
                  assert(requests > 0)
                  opened
                    .readRegion(region)
                    .map:
                      case Left(error)   => fail(error.message)
                      case Right(second) =>
                        assertEquals(shorts(second), shorts(first))
                        assertEquals(store.trace.length, requests)

    def repeatedFactored(): Future[Unit] =
      val store = directStore()
      val reader = CachingAsyncObjectReader(
        store,
        ObjectReadCache(namespace("direct-factored"), CacheLimits(16, count(4096L)))
      )
      AsyncZarr
        .openArray(reader)
        .flatMap:
          case Left(error)   => fail(error.message)
          case Right(opened) =>
            val selection = zvalue(
              FactoredSelection.within(
                opened.descriptor.shape,
                Vector(
                  AxisSelector.Indices(zvalue(AxisIndices.from(Vector(1L, 0L, 1L)))),
                  AxisSelector.Slice(zvalue(AxisSlice(0L, 4L, 2L)))
                )
              )
            )
            store.clearTrace()
            opened
              .read(selection)
              .flatMap:
                case Left(error)  => fail(error.message)
                case Right(first) =>
                  val requests = store.trace.length
                  assert(requests > 0)
                  opened
                    .read(selection)
                    .map:
                      case Left(error)   => fail(error.message)
                      case Right(second) =>
                        assertEquals(shorts(second), shorts(first))
                        assertEquals(store.trace.length, requests)

    def repeatedShard(): Future[Unit] =
      val store = zvalue(
        AsyncMemoryStore(
          Map(
            "zarr.json" -> text(ZarrBinaryFixtures.shardedStartMetadata),
            "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
          )
        )
      )
      val reader = CachingAsyncObjectReader(
        store,
        ObjectReadCache(namespace("sharded"), CacheLimits(16, count(4096L)))
      )
      AsyncZarr
        .openArray(reader)
        .flatMap:
          case Left(error)   => fail(error.message)
          case Right(opened) =>
            val region = zvalue(
              Region.within(
                opened.descriptor.shape,
                zvalue(Coordinate(0L, 0L)),
                opened.descriptor.shape
              )
            )
            store.clearTrace()
            opened
              .readRegion(region)
              .flatMap:
                case Left(error)  => fail(error.message)
                case Right(first) =>
                  val requests = store.trace.length
                  assertEquals(requests, 2)
                  opened
                    .readRegion(region)
                    .map:
                      case Left(error)   => fail(error.message)
                      case Right(second) =>
                        assertEquals(shorts(second), shorts(first))
                        assertEquals(store.trace.length, requests)

    repeatedDirectRegion().flatMap(_ => repeatedFactored()).flatMap(_ => repeatedShard())

  test("ReadLimits.maxConcurrentRequests bounds portable async fetch scheduling"):
    val metadata =
      """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{},"storage_transformers":[]}"""
    val descriptor = ZarrMetadata.parse(metadata) match
      case Right(ZarrNodeMetadata.Array(array)) => zvalue(ArrayDescriptor.compile(array))
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)
    val release = Promise[Unit]()
    var calls = 0
    var active = 0
    var maximum = 0
    val counterGate = new AnyRef
    val store = new AsyncObjectReader:
      def read(
          key: StoreKey,
          range: ByteRange
      ): Future[Either[StoreError, OwnedBytes]] =
        Future.successful(Left(StoreError.NotFound(key)))
      def readAll(
          key: StoreKey,
          maxBytes: ByteCount
      ): Future[Either[StoreError, OwnedBytes]] =
        counterGate.synchronized:
          calls += 1
          active += 1
          maximum = math.max(maximum, active)
        release.future.map: _ =>
          counterGate.synchronized:
            active -= 1
          Right(bytes(0, 0))
      def length(key: StoreKey): Future[Either[StoreError, Long]] =
        Future.successful(Left(StoreError.NotFound(key)))
    val opened = new AsyncOpenedArray(
      store,
      ZarrPath.root,
      descriptor,
      ZarrFormat.V3,
      AsyncCodecRuntime.core
    )
    val region = zvalue(
      Region.within(
        descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        descriptor.shape
      )
    )
    val reading = opened.readRegion(region, ReadLimits(maxConcurrentRequests = 2))
    counterGate.synchronized:
      assertEquals(calls, 2)
      assertEquals(maximum, 2)
    release.success(())
    reading.map:
      case Left(error)   => fail(error.message)
      case Right(result) =>
        assertEquals(result.block.elementCount, 16)
        counterGate.synchronized:
          assertEquals(calls, 16)
          assert(maximum <= 2)
