package zarr4s

import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Strong filesystem adapter around the portable create-only writer.
  *
  * The shared writer treats `zarr.json` as its object-store completion marker. This adapter
  * additionally stages the entire namespace and publishes it with one atomic directory move,
  * cleaning the stage on every refused write.
  */
object JvmZarrWriter:
  def create(
      target: Path,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable
  ): Either[ZarrError, WriteReceipt] =
    val absolute = target.toAbsolutePath.normalize()
    val parent = absolute.getParent
    if parent == null then Left(ZarrError.WriteFailure("target must have a parent directory"))
    else if Files.exists(absolute) then
      Left(ZarrError.WriteFailure(s"target already exists: $absolute"))
    else
      prepare(parent, absolute).flatMap: stage =>
        var published = false
        try
          for
            store <- JvmFileStore.open(stage).left.map(ZarrError.WriteFailure.apply)
            receipt <- SyncZarrWriter
              .create(
                store,
                descriptor,
                provider,
                limits = limits,
                runtime = runtime
              )
              .toEither
            _ <- publish(stage, absolute)
          yield
            published = true
            receipt
        catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))
        finally if !published then deleteRecursively(stage)

  private def prepare(parent: Path, target: Path): Either[ZarrError, Path] =
    try
      Files.createDirectories(parent)
      Right(Files.createTempDirectory(parent, s".${target.getFileName}.staging-"))
    catch case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))

  private def publish(stage: Path, target: Path): Either[ZarrError, Unit] =
    try
      Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE)
      Right(())
    catch
      case _: AtomicMoveNotSupportedException =>
        Left(ZarrError.WriteFailure("atomic directory publication is unavailable"))
      case NonFatal(error) => Left(ZarrError.WriteFailure(error.getMessage))

  private def deleteRecursively(root: Path): Unit =
    if Files.exists(root) then
      val stream = Files.walk(root)
      try
        stream
          .iterator()
          .asScala
          .toVector
          .sortBy(_.getNameCount)
          .reverse
          .foreach: path =>
            try Files.deleteIfExists(path)
            catch case NonFatal(_) => ()
      finally stream.close()
