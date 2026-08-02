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
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      format: ZarrFormat = ZarrFormat.V3
  ): Either[ZarrError, WriteReceipt] =
    createOutcome(target, descriptor, provider, limits, runtime, format).toEither

  /** Atomic filesystem creation retaining incomplete writer progress for typed facades. */
  def createOutcome(
      target: Path,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = JvmCodecRuntime.portable,
      format: ZarrFormat = ZarrFormat.V3
  ): WriteOutcome =
    val empty = new WriteProgress(Vector.empty, Vector.empty, 0L, 0L, 0L, 0L, ByteCount.zero)
    val absolute = target.toAbsolutePath.normalize()
    val parent = absolute.getParent
    if parent == null then
      WriteOutcome.Incomplete(empty, ZarrError.WriteFailure("target must have a parent directory"))
    else if Files.exists(absolute) then
      WriteOutcome.Incomplete(empty, ZarrError.WriteFailure(s"target already exists: $absolute"))
    else
      prepare(parent, absolute) match
        case Left(error)  => WriteOutcome.Incomplete(empty, error)
        case Right(stage) =>
          var published = false
          try
            JvmFileStore.open(stage) match
              case Left(detail) => WriteOutcome.Incomplete(empty, ZarrError.WriteFailure(detail))
              case Right(store) =>
                SyncZarrWriter
                  .create(
                    store,
                    descriptor,
                    provider,
                    limits = limits,
                    runtime = runtime,
                    format = format
                  ) match
                  case incomplete @ WriteOutcome.Incomplete(_, _) => incomplete
                  case WriteOutcome.Complete(receipt)             =>
                    publish(stage, absolute) match
                      case Left(error) => WriteOutcome.Incomplete(receipt.progress, error)
                      case Right(_)    =>
                        published = true
                        WriteOutcome.Complete(receipt)
          catch
            case NonFatal(error) =>
              WriteOutcome.Incomplete(empty, ZarrError.WriteFailure(error.getMessage))
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
