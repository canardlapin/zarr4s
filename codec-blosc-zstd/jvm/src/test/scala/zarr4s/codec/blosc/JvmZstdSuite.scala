package zarr4s.codec.blosc

import zarr4s.*

class JvmZstdSuite extends munit.FunSuite:
  private val decoded = ZstdFixtures.directDecodedChunk
  private val decodedCount = ByteCount.unsafe(decoded.length.toLong)

  test("JVM provider round-trips zstd bytes with and without a checksum"):
    List(false, true).foreach: checksum =>
      val codec = zcodec(3, checksum)
      val encoded = right(JvmZstd.encode(codec, decoded))
      assertEquals(
        right(JvmZstd.decode(codec, encoded, decodedCount, DecodeLimits.default)),
        decoded
      )

  test("JVM provider decodes an independent zstd fixture"):
    val codec = zcodec(3, false)
    assertEquals(
      right(
        JvmZstd.decode(codec, ZstdFixtures.directZstdChunk, decodedCount, DecodeLimits.default)
      ),
      decoded
    )

  test("JVM provider supports bounded decode for whole-object use"):
    val codec = zcodec(3, true)
    val encoded = right(JvmZstd.encode(codec, decoded))
    assertEquals(JvmZstd.decodeBounded(codec, encoded, DecodeLimits.default), Right(decoded))
    assert(
      JvmZstd
        .decodeBounded(codec, encoded, DecodeLimits(ByteCount.unsafe(8L)))
        .left
        .exists(_.isInstanceOf[CodecError.DecodedLimitExceeded])
    )

  test("JVM provider rejects wrong lengths, limits, and corruption"):
    val codec = zcodec(3, false)
    val encoded = right(JvmZstd.encode(codec, decoded))
    assert(JvmZstd.decode(codec, encoded, ByteCount.unsafe(11L), DecodeLimits.default).isLeft)
    assert(JvmZstd.decode(codec, encoded, decodedCount, DecodeLimits(ByteCount.unsafe(8L))).isLeft)
    val corrupt = encoded.toArray
    corrupt(0) = (corrupt(0) ^ 1).toByte
    assert(
      JvmZstd.decode(codec, OwnedBytes.copyOf(corrupt), decodedCount, DecodeLimits.default).isLeft
    )

  test("JVM provider reads a v2 zstd array"):
    val store = zvalue(
      MemoryStore(
        Map(
          ".zarray" -> OwnedBytes.copyOf(ZstdFixtures.v2ArrayZstd.getBytes("UTF-8")),
          "0.0" -> ZstdFixtures.directZstdChunk
        )
      )
    )
    val opened = zvalue(
      SyncZarr.openArray(
        store,
        capabilities = BloscZstdProvider.capabilities(),
        runtime = JvmBloscZstdRuntime.portable
      )
    )
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    zvalue(opened.readRegion(region)).block match
      case PrimitiveBlock.Int16(values) =>
        assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
      case _ => fail("expected int16 result")

  test("JVM provider writer publishes a v2 zstd compressor"):
    val descriptor = zvalue(
      ZarrMetadata
        .parse(ZstdFixtures.v3ArrayZstd)
        .flatMap:
          case ZarrNodeMetadata.Array(array) =>
            ArrayDescriptor.compile(array, BloscZstdProvider.capabilities())
          case _ => Left(ZarrError.UnsupportedNodeType("group"))
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
      runtime = JvmBloscZstdRuntime.portable,
      format = ZarrFormat.V2
    )
    val receipt = outcome match
      case WriteOutcome.Complete(found)      => found
      case WriteOutcome.Incomplete(_, error) => fail(error.message)
    assertEquals(store.writeTrace.map(_.key.value), Vector(".zattrs", "0/0", ".zarray"))
    assertEquals(receipt.metadata.key.value, ".zarray")
    val opened = zvalue(
      SyncZarr.openArray(
        store,
        capabilities = BloscZstdProvider.capabilities(),
        runtime = JvmBloscZstdRuntime.portable
      )
    )
    val region =
      zvalue(Region.within(descriptor.shape, zvalue(Coordinate(0L, 0L)), descriptor.shape))
    zvalue(opened.readRegion(region)).block match
      case PrimitiveBlock.Int16(values) =>
        assertEquals(values.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
      case _ => fail("expected int16 result")

  test("optional runtime advertises zstd without changing the default JVM runtime"):
    assert(!JvmCodecRuntime.portable.executorNames.contains("zstd"))
    assert(JvmBloscZstdRuntime.portable.executorNames.contains("zstd"))

  private def zcodec(level: Int, checksum: Boolean): ZstdCodec =
    ZstdCodec.create(level, checksum) match
      case Right(found) => found
      case Left(error)  => fail(error)

  private def right[A](value: Either[CodecError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def zvalue[A](value: Either[ZarrError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)
