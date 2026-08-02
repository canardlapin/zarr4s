package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global

class JvmZlibSuite extends munit.FunSuite:
  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  test("JVM zlib round-trips owned bytes with exact length"):
    val decoded = OwnedBytes.copyOf(Array.fill[Byte](4096)(7))
    val encoded = JvmZlib.encode(decoded) match
      case Right(found) => found
      case Left(error)  => fail(error.message)
    assert(encoded.length < decoded.length)
    assertEquals(JvmZlib.decode(encoded, count(decoded.length)), Right(decoded))

  test("JVM zlib rejects wrong lengths, limits, and corruption"):
    val decoded = OwnedBytes.copyOf(Array.fill[Byte](1024)(7))
    val encoded = JvmZlib.encode(decoded).toOption.get
    assert(JvmZlib.decode(encoded, count(1023)).isLeft)
    assert(JvmZlib.decode(encoded, count(1024), DecodeLimits(count(100))).isLeft)
    val corrupt = encoded.toArray
    corrupt(corrupt.length / 2) = (corrupt(corrupt.length / 2) ^ 1).toByte
    assert(JvmZlib.decode(OwnedBytes.copyOf(corrupt), count(1024)).isLeft)

  test("JVM asynchronous runtime exposes zlib"):
    assert(JvmAsyncCodecRuntime.portable(global).executorNames.contains("zlib"))

  test("JVM decodes a Python zlib chunk through v2 metadata lowering"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> HierarchyFixtures.bytes(HierarchyFixtures.v2ArrayZlib),
          "0.0" -> ZarrBinaryFixtures.directZlibChunk
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    val result = zvalue(opened.readRegion(region))
    result.block match
      case PrimitiveBlock.Int16(values) =>
        assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
      case _ => fail("expected int16 result")

  test("JVM writer publishes a v2 zlib compressor"):
    val descriptor = zvalue(
      ZarrMetadata
        .parse(
          """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"zlib","configuration":{"level":1}}],"attributes":{},"storage_transformers":[]}"""
        )
        .flatMap:
          case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array)
          case _                             => Left(ZarrError.UnsupportedNodeType("group"))
    )
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        Right(
          ChunkPayload.Values(
            PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, -2, 300, 4, 5, -6)))
          )
        )
    val store = zvalue(MemoryStore(Map.empty))
    val outcome = SyncZarrWriter.create(
      store,
      descriptor,
      provider,
      runtime = JvmCodecRuntime.portable,
      format = ZarrFormat.V2
    )
    val receipt = outcome match
      case WriteOutcome.Complete(found)      => found
      case WriteOutcome.Incomplete(_, error) => fail(error.message)
    assertEquals(store.writeTrace.map(_.key.value), Vector(".zattrs", "0/0", ".zarray"))
    assertEquals(receipt.metadata.key.value, ".zarray")
    val opened = zvalue(SyncZarr.openArray(store, runtime = JvmCodecRuntime.portable))
    val region =
      zvalue(Region.within(descriptor.shape, zvalue(Coordinate(0L, 0L)), descriptor.shape))
    zvalue(opened.readRegion(region)).block match
      case PrimitiveBlock.Int16(values) =>
        assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
      case _ => fail("expected int16 result")
