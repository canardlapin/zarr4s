package zarr4s

import java.nio.file.Files
import java.nio.file.Path
import scala.jdk.CollectionConverters.*
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

class JvmWriterSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def descriptor(metadata: String): ArrayDescriptor =
    val parsed = zvalue(ZarrMetadata.parse(metadata)) match
      case ZarrNodeMetadata.Array(array) => array
      case _                             => fail("expected array metadata")
    zvalue(ArrayDescriptor.compile(parsed))

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  private val directMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[4,5],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":-9,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":1}},{"name":"crc32c"}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  private val outerShardedMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"sharding_indexed","configuration":{"chunk_shape":[2,2],"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"shuffle","configuration":{"elementsize":8}},{"name":"crc32c"}],"index_location":"start"}},{"name":"gzip","configuration":{"level":1}}],"attributes":{},"storage_transformers":[]}"""

  test("direct writer is create-only, deterministic, and readable"):
    val parent = Files.createTempDirectory("zarr4s-core-writer-direct")
    val first = parent.resolve("first.zarr")
    val second = parent.resolve("second.zarr")
    val found = descriptor(directMetadata)
    val provider = gridProvider(
      found.grid,
      columns = 5,
      fillValue = -9,
      fillCoordinate = Some(Vector(1L, 0L))
    )
    val firstReceipt = zvalue(JvmZarrWriter.create(first, found, provider))
    val secondReceipt = zvalue(JvmZarrWriter.create(second, found, provider))
    assertEquals(snapshot(first), snapshot(second))
    assertEquals(
      firstReceipt.objects.map(objectValue => objectValue.key.value -> objectValue.sha256),
      secondReceipt.objects.map(objectValue => objectValue.key.value -> objectValue.sha256)
    )
    assertEquals(firstReceipt.omittedFillChunks, 1L)
    assert(JvmZarrWriter.create(first, found, provider).isLeft)

    val store = JvmFileStore.open(first).fold(fail(_), identity)
    val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
    val region = zvalue(Region.within(found.shape, zvalue(Coordinate(0L, 0L)), found.shape))
    val result = zvalue(opened.readRegion(region))
    assertEquals(
      shorts(result),
      Vector[Short](
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -9, -9, -9, 13, 14, -9, -9, -9, 18, 19
      )
    )

  test("start-indexed sharded writer reproduces the Python object"):
    val metadata = ZarrBinaryFixtures.shardedStartMetadata
    val base = descriptor(metadata)
    val sharded = base.layout match
      case PhysicalLayout.Sharded(grid, _, _, _, _) => grid
      case _                                        => fail("expected sharded layout")
    val parent = Files.createTempDirectory("zarr4s-core-writer-sharded")
    val target = parent.resolve("array.zarr")
    val provider = new ChunkProvider:
      def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
        coordinate.toVector match
          case Vector(0L, 0L) => Right(ChunkPayload.Values(int16(1, 2, 3, 4)))
          case Vector(1L, 1L) => Right(ChunkPayload.Values(int16(13, 14, 15, 16)))
          case _              => Right(ChunkPayload.Fill)
    val receipt = zvalue(JvmZarrWriter.create(target, base, provider))
    assertEquals(receipt.objects.map(_.key.value), Vector("c/0/0"))
    assertEquals(
      OwnedBytes.copyOf(Files.readAllBytes(target.resolve("c/0/0"))),
      ZarrBinaryFixtures.shardedStartObject
    )
    assertEquals(sharded.innerChunksPerShard.toVector, Vector(2L, 2L))

  test("outer gzip sharding uses a bounded whole-shard read and fixed-size index stages"):
    val found = descriptor(outerShardedMetadata)
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] = coordinate.toVector match
        case Vector(0L, 0L) => Right(ChunkPayload.Values(int16(1, 2, 3, 4)))
        case Vector(1L, 1L) => Right(ChunkPayload.Values(int16(13, 14, 15, 16)))
        case _              => Right(ChunkPayload.Fill)
    val store = zvalue(MemoryStore(Map.empty))
    val receipt = zvalue(
      SyncZarrWriter.create(
        store,
        found,
        provider,
        runtime = JvmCodecRuntime.portable
      ) match
        case WriteOutcome.Complete(value)      => Right(value)
        case WriteOutcome.Incomplete(_, error) => Left(error)
    )
    val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
    val region = zvalue(Region.within(found.shape, zvalue(Coordinate(0L, 0L)), found.shape))
    val result = zvalue(opened.readRegion(region))
    assertEquals(
      shorts(result),
      Vector[Short](1, 2, 0, 0, 3, 4, 0, 0, 0, 0, 13, 14, 0, 0, 15, 16)
    )
    assertEquals(result.receipt.objectRequests, 1)
    assertEquals(result.receipt.rangeRequests, 0)
    assertEquals(result.receipt.lengthRequests, 0)
    assertEquals(result.receipt.indexBytesRead, 0L)
    assertEquals(result.receipt.dataBytesRead, result.receipt.bytesRead)
    assertEquals(receipt.objects.map(_.key.value), Vector("c/0/0"))

  test("async outer sharding writes and reads through bounded whole-shard fallback"):
    val found = descriptor(outerShardedMetadata)
    val provider = new AsyncChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      )(using ExecutionContext): scala.concurrent.Future[Either[ZarrError, ChunkPayload]] =
        val payload = coordinate.toVector match
          case Vector(0L, 0L) => ChunkPayload.Values(int16(1, 2, 3, 4))
          case Vector(1L, 1L) => ChunkPayload.Values(int16(13, 14, 15, 16))
          case _              => ChunkPayload.Fill
        scala.concurrent.Future.successful(Right(payload))
    val store = zvalue(AsyncMemoryStore(Map.empty))
    val runtime = JvmAsyncCodecRuntime.portable(global)
    AsyncZarrWriter
      .create(store, found, provider, runtime = runtime)
      .flatMap:
        case WriteOutcome.Incomplete(_, error) =>
          scala.concurrent.Future.failed(new AssertionError(error.message))
        case WriteOutcome.Complete(_) =>
          val region = zvalue(Region.within(found.shape, zvalue(Coordinate(0L, 0L)), found.shape))
          AsyncZarr
            .openArray(store, runtime = runtime)
            .flatMap:
              case Left(error) => scala.concurrent.Future.failed(new AssertionError(error.message))
              case Right(opened) =>
                opened
                  .readRegion(region)
                  .map:
                    case Left(error)   => fail(error.message)
                    case Right(result) =>
                      val values = result.block match
                        case PrimitiveBlock.Int16(found) => found.toArray.toVector
                        case _                           => fail("expected int16 result")
                      assertEquals(
                        values,
                        Vector[Short](1, 2, 0, 0, 3, 4, 0, 0, 0, 0, 13, 14, 0, 0, 15, 16)
                      )
                      assertEquals(result.receipt.objectRequests, 1)
                      assertEquals(result.receipt.rangeRequests, 0)

  test("whole-shard fallback fails closed on outer corruption and decoded-size limits"):
    val found = descriptor(outerShardedMetadata)
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] = coordinate.toVector match
        case Vector(0L, 0L) => Right(ChunkPayload.Values(int16(1, 2, 3, 4)))
        case _              => Right(ChunkPayload.Fill)
    val source = zvalue(MemoryStore(Map.empty))
    zvalue(
      SyncZarrWriter.create(
        source,
        found,
        provider,
        runtime = JvmCodecRuntime.portable
      ) match
        case WriteOutcome.Complete(value)      => Right(value)
        case WriteOutcome.Incomplete(_, error) => Left(error)
    )
    val corruptedBytes = source.snapshot("c/0/0").toArray
    corruptedBytes(0) = (corruptedBytes(0) ^ 1).toByte
    val corrupted = zvalue(
      MemoryStore(source.snapshot.updated("c/0/0", OwnedBytes.copyOf(corruptedBytes)))
    )
    val region = zvalue(Region.within(found.shape, zvalue(Coordinate(0L, 0L)), found.shape))
    val openedCorrupted = zvalue(SyncZarr.openArray(corrupted, runtime = JvmCodecRuntime.portable))
    assert(openedCorrupted.readRegion(region).isLeft)

    val limited = zvalue(SyncZarr.openArray(source, runtime = JvmCodecRuntime.portable))
    assert(
      limited
        .readRegion(
          region,
          ReadLimits(
            shardIndex = ShardIndexLimits(maxShardBytes = ByteCount.unsafe(8L))
          )
        )
        .isLeft
    )

  test("provider failure never publishes a partial target and staging is cleaned"):
    val parent = Files.createTempDirectory("zarr4s-core-writer-interrupt")
    val target = parent.resolve("interrupted.zarr")
    val found = descriptor(directMetadata)
    val provider = new ChunkProvider:
      def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
        if coordinate.toVector == Vector(0L, 0L) then
          Right(ChunkPayload.Values(int16(0, 1, 2, 5, 6, 7)))
        else Left(ZarrError.WriteFailure("simulated provider interruption"))
    assert(JvmZarrWriter.create(target, found, provider).isLeft)
    assert(!Files.exists(target))
    val stream = Files.list(parent)
    try assertEquals(stream.iterator().asScala.toVector, Vector.empty)
    finally stream.close()

  private def gridProvider(
      grid: RegularGrid,
      columns: Int,
      fillValue: Short,
      fillCoordinate: Option[Vector[Long]]
  ): ChunkProvider = new ChunkProvider:
    def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
      if fillCoordinate.contains(coordinate.toVector) then Right(ChunkPayload.Fill)
      else
        val rows = storedShape.axis(0).toInt
        val width = storedShape.axis(1).toInt
        val originRow = coordinate.axis(0).toInt * grid.chunkShape.axis(0).toInt
        val originColumn = coordinate.axis(1).toInt * grid.chunkShape.axis(1).toInt
        val values = Array.fill[Short](rows * width)(fillValue)
        var row = 0
        while row < rows do
          var column = 0
          while column < width do
            if originRow + row < grid.arrayShape.axis(0) &&
              originColumn + column < grid.arrayShape.axis(1)
            then
              values(row * width + column) =
                ((originRow + row) * columns + originColumn + column).toShort
            column += 1
          row += 1
        Right(ChunkPayload.Values(PrimitiveBlock.Int16(OwnedShorts.copyOf(values))))

  private def int16(values: Short*): PrimitiveBlock =
    PrimitiveBlock.Int16(OwnedShorts.copyOf(values.toArray))

  private def snapshot(root: Path): Vector[(String, Vector[Byte])] =
    val stream = Files.walk(root)
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map: path =>
          root.relativize(path).toString.replace('\\', '/') -> Files.readAllBytes(path).toVector
        .toVector
        .sortBy(_._1)
    finally stream.close()
