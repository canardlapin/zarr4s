package zarr4s.codec.blosc

import zarr4s.*
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class BrowserBloscZstdSuite extends munit.FunSuite:
  private val decoded = OwnedBytes.copyOf(Array.tabulate[Byte](4096): index =>
    ((index / 4) % 31).toByte)
  private val decodedCount = ByteCount.unsafe(decoded.length.toLong)

  test("Scala.js provider round-trips the observed typesize-four surface"):
    Future.sequence(BloscShuffle.values.toVector.map: shuffle =>
      val codec = zcodec(5, shuffle, 4, 0)
      BrowserBloscZstd
        .encode(codec, decoded)
        .flatMap:
          case Left(error)    => fail(error.message)
          case Right(encoded) =>
            BrowserBloscZstd
              .decode(
                codec,
                encoded,
                decodedCount,
                DecodeLimits.default
              )
              .map: restored =>
                assertEquals(right(restored), decoded))

  test("Scala.js provider refuses metadata-inconsistent non-four-byte writes"):
    val codec = zcodec(5, BloscShuffle.ByteShuffle, 1, 0)
    BrowserBloscZstd
      .encode(codec, decoded)
      .map: result =>
        assert(result.left.exists(_.message.contains("encode with typesize 1")))

  test("Scala.js provider checks hostile frame lengths before WebAssembly"):
    val codec = zcodec(5, BloscShuffle.ByteShuffle, 4, 0)
    BrowserBloscZstd
      .encode(codec, decoded)
      .flatMap:
        case Left(error)    => fail(error.message)
        case Right(encoded) =>
          val forged = encoded.toArray
          putUInt32(forged, 4, 1024L * 1024L * 1024L)
          BrowserBloscZstd
            .decode(
              codec,
              OwnedBytes.copyOf(forged),
              decodedCount,
              DecodeLimits(ByteCount.unsafe(8192L))
            )
            .map: result =>
              assertEquals(
                result,
                Left(
                  CodecError.InvalidDecodedLength(
                    decoded.length.toLong,
                    1024L * 1024L * 1024L
                  )
                )
              )

  test("runtime advertises Blosc without changing the default browser runtime"):
    assert(!BrowserCodecRuntime.portable.executorNames.contains("blosc"))
    assert(BrowserBloscZstdRuntime.portable.executorNames.contains("blosc"))
    assert(BrowserBloscZstdRuntime.portable.executorNames.contains("gzip"))

  test("Scala.js reads direct and indexed-shard objects emitted by Zarr-Python"):
    for
      direct <- readAll(BloscPythonFixtures.directObjects)
      sharded <- readAll(BloscPythonFixtures.shardedObjects)
    yield
      assertEquals(direct, BloscPythonFixtures.directValues)
      assertEquals(sharded, BloscPythonFixtures.shardedValues)

  test("Scala.js reads Python shuffled int16 despite its fixed-width encoder"):
    readShorts(BloscPythonFixtures.int16Objects).map: values =>
      assertEquals(values, BloscPythonFixtures.int16Values)

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

  private def readAll(objects: Map[String, OwnedBytes]): Future[Vector[Float]] =
    val store = zvalue(AsyncMemoryStore(objects))
    BrowserZarr
      .openArray(
        store,
        capabilities = BloscZstdProvider.capabilities(),
        runtime = BrowserBloscZstdRuntime.portable
      )
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
          opened
            .readRegion(region)
            .map:
              case Left(error)   => fail(error.message)
              case Right(result) =>
                result.block match
                  case PrimitiveBlock.Float32(values) => values.toArray.toVector
                  case _                              => fail("expected float32 result")

  private def zvalue[A](value: Either[ZarrError, A]): A = value match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def readShorts(objects: Map[String, OwnedBytes]): Future[Vector[Short]] =
    val store = zvalue(AsyncMemoryStore(objects))
    BrowserZarr
      .openArray(
        store,
        capabilities = BloscZstdProvider.capabilities(),
        runtime = BrowserBloscZstdRuntime.portable
      )
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
          opened
            .readRegion(region)
            .map:
              case Left(error)   => fail(error.message)
              case Right(result) =>
                result.block match
                  case PrimitiveBlock.Int16(values) => values.toArray.toVector
                  case _                            => fail("expected int16 result")

  private def putUInt32(bytes: Array[Byte], offset: Int, value: Long): Unit =
    var index = 0
    while index < 4 do
      bytes(offset + index) = ((value >>> (index * 8)) & 0xffL).toByte
      index += 1
