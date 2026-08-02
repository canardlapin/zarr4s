package zarr4s.codec.blosc

import java.nio.file.Files
import java.nio.file.Path
import zarr4s.*

object BloscWriterFixtureMain:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 1, "expected output parent")
    val parent = Path.of(arguments(0))
    Files.createDirectories(parent)
    writeDirect(parent.resolve("scala-blosc-direct.zarr"))
    writeSharded(parent.resolve("scala-blosc-sharded.zarr"))
    writeV2(parent.resolve("scala-blosc-v2.zarr"))

  private def writeDirect(target: Path): Unit =
    val values = Array[Float](1.25f, -2.5f, 300.0f, 4.5f, 5.75f, -6.0f)
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        Right(ChunkPayload.Values(float32(values)))
    write(target, descriptor(BloscPythonFixtures.directMetadata), provider)

  private def writeSharded(target: Path): Unit =
    val chunks = Vector(
      Array[Float](1f, 2f, 5f, 6f),
      Array[Float](3f, 4f, 7f, 8f),
      Array[Float](9f, 10f, 13f, 14f),
      Array[Float](11f, 12f, 15f, 16f)
    )
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        val index = (coordinate.axis(0) * 2L + coordinate.axis(1)).toInt
        Right(ChunkPayload.Values(float32(chunks(index))))
    write(target, descriptor(BloscPythonFixtures.shardedMetadata), provider)

  private def writeV2(target: Path): Unit =
    val values = Array[Float](1.25f, -2.5f, 300.0f, 4.5f, 5.75f, -6.0f)
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        Right(ChunkPayload.Values(float32(values)))
    write(
      target,
      descriptor(BloscPythonFixtures.directMetadata),
      provider,
      format = ZarrFormat.V2
    )

  private def descriptor(metadata: String): ArrayDescriptor =
    BloscPythonFixtures.descriptor(metadata) match
      case Right(found) => found
      case Left(error)  => throw new IllegalArgumentException(error.message)

  private def float32(values: Array[Float]): PrimitiveBlock =
    PrimitiveBlock.Float32(OwnedFloats.copyOf(values))

  private def write(
      target: Path,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      format: ZarrFormat = ZarrFormat.V3
  ): Unit = JvmZarrWriter.create(
    target,
    descriptor,
    provider,
    runtime = JvmBloscZstdRuntime.portable,
    format = format
  ) match
    case Right(_)    => ()
    case Left(error) => throw new IllegalStateException(error.message)
