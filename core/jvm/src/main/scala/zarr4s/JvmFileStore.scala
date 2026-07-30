package zarr4s

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import scala.util.control.NonFatal

final class JvmFileStore private (root: Path) extends ObjectReader, ObjectWriter:
  def read(key: StoreKey, range: ByteRange): Either[StoreError, OwnedBytes] =
    resolveExisting(key).flatMap: path =>
      try
        val size = Files.size(path)
        range.endExclusive match
          case Left(_) => Left(StoreError.RangeNotSatisfiable(key, range, size))
          case Right(end) if end > size || range.length.toLong > Int.MaxValue.toLong =>
            Left(StoreError.RangeNotSatisfiable(key, range, size))
          case Right(_) =>
            val bytes = new Array[Byte](range.length.toLong.toInt)
            val channel = FileChannel.open(path, StandardOpenOption.READ)
            try
              val buffer = ByteBuffer.wrap(bytes)
              var position = range.offset
              while buffer.hasRemaining do
                val count = channel.read(buffer, position)
                if count < 0 then throw new EOFException(s"unexpected EOF at $position")
                position += count.toLong
              Right(OwnedBytes.unsafe(bytes))
            finally channel.close()
      catch
        case _: NoSuchFileException => Left(StoreError.NotFound(key))
        case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = false))

  def readAll(key: StoreKey, maxBytes: ByteCount): Either[StoreError, OwnedBytes] =
    resolveExisting(key).flatMap: path =>
      try
        val size = Files.size(path)
        if size > maxBytes.toLong || size > Int.MaxValue.toLong then
          Left(StoreError.ObjectTooLarge(key, math.min(maxBytes.toLong, Int.MaxValue.toLong), size))
        else Right(OwnedBytes.unsafe(Files.readAllBytes(path)))
      catch
        case _: NoSuchFileException => Left(StoreError.NotFound(key))
        case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = false))

  def length(key: StoreKey): Either[StoreError, Long] =
    resolveExisting(key).flatMap: path =>
      try Right(Files.size(path))
      catch
        case _: NoSuchFileException => Left(StoreError.NotFound(key))
        case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = false))

  def create(key: StoreKey, bytes: OwnedBytes): Either[StoreError, Unit] =
    resolveTarget(key).flatMap: path =>
      val parent = path.getParent
      var staged: Option[Path] = None
      try
        Files.createDirectories(parent)
        val realParent = parent.toRealPath()
        if !realParent.startsWith(root) then Left(StoreError.Forbidden(key))
        else
          val temporary = Files.createTempFile(realParent, ".zarr-object-", ".tmp")
          staged = Some(temporary)
          Files.write(temporary, bytes.values)
          Files.createLink(path, temporary)
          Right(())
      catch
        case _: FileAlreadyExistsException => Left(StoreError.AlreadyExists(key))
        case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = false))
      finally
        staged.foreach: temporary =>
          try Files.deleteIfExists(temporary)
          catch case NonFatal(_) => ()

  private def resolveExisting(key: StoreKey): Either[StoreError, Path] =
    try
      val candidate = root.resolve(key.value).normalize()
      if !candidate.startsWith(root) then Left(StoreError.Forbidden(key))
      else
        val real = candidate.toRealPath()
        if real.startsWith(root) then Right(real)
        else Left(StoreError.Forbidden(key))
    catch
      case _: NoSuchFileException => Left(StoreError.NotFound(key))
      case NonFatal(error) => Left(StoreError.Transport(key, error.getMessage, transient = false))

  private def resolveTarget(key: StoreKey): Either[StoreError, Path] =
    try
      val candidate = root.resolve(key.value).normalize()
      if candidate.startsWith(root) then Right(candidate)
      else Left(StoreError.Forbidden(key))
    catch
      case NonFatal(error) =>
        Left(StoreError.Transport(key, error.getMessage, transient = false))

object JvmFileStore:
  def open(root: Path): Either[String, JvmFileStore] =
    try
      val real = root.toRealPath()
      if !Files.isDirectory(real) then Left(s"store root is not a directory: $real")
      else Right(new JvmFileStore(real))
    catch case NonFatal(error) => Left(error.getMessage)
