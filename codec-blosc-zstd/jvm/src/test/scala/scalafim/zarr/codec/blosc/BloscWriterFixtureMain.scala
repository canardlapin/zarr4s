package scalafim.zarr.codec.blosc

import java.nio.file.Files
import java.nio.file.Path
import scalafim.zarr.*

object BloscWriterFixtureMain:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 1, "expected output parent")
    val parent = Path.of(arguments(0))
    Files.createDirectories(parent)
    writeDirect(parent.resolve("scala-blosc-direct.zarr"))
    writeSharded(parent.resolve("scala-blosc-sharded.zarr"))

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

  private def descriptor(metadata: String): ArrayDescriptor =
    BloscPythonFixtures.descriptor(metadata) match
      case Right(found) => found
      case Left(error) => throw new IllegalArgumentException(error.message)

  private def float32(values: Array[Float]): PrimitiveBlock =
    PrimitiveBlock.Float32(OwnedFloats.copyOf(values))

  private def write(
      target: Path,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider
  ): Unit = JvmZarrWriter.create(
    target,
    descriptor,
    provider,
    runtime = JvmBloscZstdRuntime.portable
  ) match
    case Right(_) => ()
    case Left(error) => throw new IllegalStateException(error.message)
