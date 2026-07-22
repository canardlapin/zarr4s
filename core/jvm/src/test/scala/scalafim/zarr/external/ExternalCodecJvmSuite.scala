package scalafim.zarr.external

import java.nio.file.Files
import scalafim.zarr.*

class ExternalCodecJvmSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _ => fail("expected int16 result")

  test("external metadata and executor capabilities fail independently at open"):
    val store = zvalue(MemoryStore(ExternalXorFixture.directObjects))
    assertEquals(
      SyncZarr.openArray(store).left.map(_.message),
      Left("unsupported codec extension 'org.scalafim.test.xor'")
    )
    assertEquals(
      SyncZarr.openArray(
        store,
        capabilities = ExternalXorFixture.capabilities
      ).left.map(_.message),
      Left("org.scalafim.test.xor is unavailable on synchronous runtime")
    )

  test("external provider reads and writes a direct array without core branching"):
    val memory = zvalue(MemoryStore(ExternalXorFixture.directObjects))
    val opened = zvalue(SyncZarr.openArray(
      memory,
      capabilities = ExternalXorFixture.capabilities,
      runtime = ExternalXorFixture.syncRuntime
    ))
    val region = zvalue(Region.within(
      opened.descriptor.shape,
      zvalue(Coordinate(0L, 0L)),
      opened.descriptor.shape
    ))
    assertEquals(shorts(zvalue(opened.readRegion(region))), Vector[Short](1, 2, 3, 4, 5, 6))

    val root = Files.createTempDirectory("scalafim-external-codec-direct")
    val target = root.resolve("array.zarr")
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] = Right(ChunkPayload.Values(
        PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, 2, 3, 4, 5, 6)))
      ))
    zvalue(JvmZarrWriter.create(
      target,
      zvalue(ExternalXorFixture.descriptor(ExternalXorFixture.directMetadata)),
      provider,
      runtime = ExternalXorFixture.syncRuntime
    ))
    assertEquals(
      OwnedBytes.copyOf(Files.readAllBytes(target.resolve("c/0/0"))),
      ExternalXorFixture.directChunk
    )
    val rendered = Files.readString(target.resolve("zarr.json"))
    assert(rendered.contains("\"name\":\"org.scalafim.test.xor\""))
    assert(rendered.contains("\"mask\":90"))

  test("external provider reads and writes an indexed shard"):
    val memory = zvalue(MemoryStore(ExternalXorFixture.shardedObjects))
    val opened = zvalue(SyncZarr.openArray(
      memory,
      capabilities = ExternalXorFixture.capabilities,
      runtime = ExternalXorFixture.syncRuntime
    ))
    val region = zvalue(Region.within(
      opened.descriptor.shape,
      zvalue(Coordinate(0L, 0L)),
      opened.descriptor.shape
    ))
    assertEquals(shorts(zvalue(opened.readRegion(region))), ExternalXorFixture.fullValues)

    val root = Files.createTempDirectory("scalafim-external-codec-sharded")
    val target = root.resolve("array.zarr")
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        val index = (coordinate.axis(0) * 2L + coordinate.axis(1)).toInt
        Right(ChunkPayload.Values(ExternalXorFixture.shardedChunks(index)))
    zvalue(JvmZarrWriter.create(
      target,
      zvalue(ExternalXorFixture.descriptor(ExternalXorFixture.shardedMetadata)),
      provider,
      runtime = ExternalXorFixture.syncRuntime
    ))
    assertEquals(
      OwnedBytes.copyOf(Files.readAllBytes(target.resolve("c/0/0"))),
      ExternalXorFixture.shardedObject
    )
