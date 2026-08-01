package zarr4s.codec.blosc

import zarr4s.*

class JvmBloscZstdSuite extends munit.FunSuite:
  private val decoded = OwnedBytes.copyOf(Array.tabulate[Byte](4096): index =>
    ((index / 4) % 31).toByte)
  private val decodedCount = ByteCount.unsafe(decoded.length.toLong)

  test("JVM provider round-trips every Zarr shuffle mode"):
    BloscShuffle.values.foreach: shuffle =>
      val codec = zcodec(5, shuffle, 4, 0)
      val encoded = right(JvmBloscZstd.encode(codec, decoded))
      val restored = right(
        JvmBloscZstd.decode(
          codec,
          encoded,
          decodedCount,
          DecodeLimits.default
        )
      )
      assertEquals(restored, decoded)
      assert(encoded.length < decoded.length, s"$shuffle should compress this fixture")

  test("JVM provider rejects a forged decoded length before JNI"):
    val codec = zcodec(5, BloscShuffle.ByteShuffle, 4, 0)
    val encoded = right(JvmBloscZstd.encode(codec, decoded))
    val forged = encoded.toArray
    putUInt32(forged, 4, 1024L * 1024L * 1024L)

    val result = JvmBloscZstd.decode(
      codec,
      OwnedBytes.copyOf(forged),
      decodedCount,
      DecodeLimits(ByteCount.unsafe(8192L))
    )
    assertEquals(
      result,
      Left(CodecError.InvalidDecodedLength(decoded.length.toLong, 1024L * 1024L * 1024L))
    )

  test("JVM provider rejects truncated and inconsistent frames"):
    val codec = zcodec(5, BloscShuffle.BitShuffle, 4, 0)
    val short = JvmBloscZstd.decode(
      codec,
      OwnedBytes.copyOf(Array.fill[Byte](15)(0)),
      decodedCount,
      DecodeLimits.default
    )
    assert(short.left.exists(_.message.contains("header requires 16 bytes")))

    val encoded = right(JvmBloscZstd.encode(codec, decoded))
    val wrongTypeSize = encoded.toArray
    wrongTypeSize(3) = 8.toByte
    val mismatch = JvmBloscZstd.decode(
      codec,
      OwnedBytes.copyOf(wrongTypeSize),
      decodedCount,
      DecodeLimits.default
    )
    assert(mismatch.left.exists(_.message.contains("does not match metadata")))

  test("runtime advertises Blosc without changing the default JVM runtime"):
    assert(!JvmCodecRuntime.portable.executorNames.contains("blosc"))
    assert(JvmBloscZstdRuntime.portable.executorNames.contains("blosc"))
    assert(JvmBloscZstdRuntime.portable.executorNames.contains("gzip"))
    assert(JvmBloscZstdRuntime.portable.executorNames.contains("zstd"))

  test("JVM reads direct and indexed-shard objects emitted by Zarr-Python"):
    assertEquals(readAll(BloscPythonFixtures.directObjects), BloscPythonFixtures.directValues)
    assertEquals(readAll(BloscPythonFixtures.shardedObjects), BloscPythonFixtures.shardedValues)

  test("JVM reads Python shuffled int16 with typesize two"):
    assertEquals(readShorts(BloscPythonFixtures.int16Objects), BloscPythonFixtures.int16Values)

  private def zcodec(
      level: Int,
      shuffle: BloscShuffle,
      typeSize: Int,
      blockSize: Int
  ): BloscZstdCodec = BloscZstdCodec.create(level, shuffle, typeSize, blockSize) match
    case Right(found) => found
    case Left(error)  => fail(error)

  private def right[A](value: Either[CodecError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def readAll(objects: Map[String, OwnedBytes]): Vector[Float] =
    val store = zvalue(MemoryStore(objects))
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
      case PrimitiveBlock.Float32(values) => values.toArray.toVector
      case _                              => fail("expected float32 result")

  private def zvalue[A](value: Either[ZarrError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def readShorts(objects: Map[String, OwnedBytes]): Vector[Short] =
    val store = zvalue(MemoryStore(objects))
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
      case PrimitiveBlock.Int16(values) => values.toArray.toVector
      case _                            => fail("expected int16 result")

  private def putUInt32(bytes: Array[Byte], offset: Int, value: Long): Unit =
    var index = 0
    while index < 4 do
      bytes(offset + index) = ((value >>> (index * 8)) & 0xffL).toByte
      index += 1
