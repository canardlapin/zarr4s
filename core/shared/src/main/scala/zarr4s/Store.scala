package zarr4s

import scala.collection.mutable
import scala.concurrent.Future

enum StoreError:
  case Unauthorized(key: StoreKey)
  case Forbidden(key: StoreKey)
  case NotFound(key: StoreKey)
  case AlreadyExists(key: StoreKey)
  case RangeNotSatisfiable(key: StoreKey, range: ByteRange, objectLength: Long)
  case RangeIgnored(key: StoreKey, range: ByteRange)
  case ObjectTooLarge(key: StoreKey, limit: Long, actual: Long)
  case ObjectLengthUnavailable(key: StoreKey)
  case ListingTooLarge(prefix: ZarrPath, limit: Int)
  case ListingFailure(prefix: ZarrPath, detail: String)
  case Transport(key: StoreKey, detail: String, transient: Boolean)

  def message: String = this match
    case Unauthorized(key)  => s"unauthorized store object ${key.value}"
    case Forbidden(key)     => s"forbidden store object ${key.value}"
    case NotFound(key)      => s"store object not found: ${key.value}"
    case AlreadyExists(key) => s"store object already exists: ${key.value}"
    case RangeNotSatisfiable(key, range, objectLength) =>
      s"range ${range.offset}:${range.length.toLong} exceeds ${key.value} length $objectLength"
    case RangeIgnored(key, range) =>
      s"store ignored range ${range.offset}:${range.length.toLong} for ${key.value}"
    case ObjectTooLarge(key, limit, actual) =>
      s"store object ${key.value} exceeds limit $limit with $actual bytes"
    case ObjectLengthUnavailable(key)   => s"object length unavailable for ${key.value}"
    case ListingTooLarge(prefix, limit) =>
      s"store listing for '${prefix.value}' exceeds limit $limit"
    case ListingFailure(prefix, detail) =>
      s"store listing failed for '${prefix.value}': $detail"
    case Transport(key, detail, transient) =>
      val kind = if transient then "transient" else "permanent"
      s"$kind store failure for ${key.value}: $detail"

enum ObjectRequest:
  case Whole(key: StoreKey)
  case Range(key: StoreKey, range: ByteRange)
  case Length(key: StoreKey)

trait ObjectReader:
  def read(key: StoreKey, range: ByteRange): Either[StoreError, OwnedBytes]
  def readAll(key: StoreKey, maxBytes: ByteCount): Either[StoreError, OwnedBytes]
  def length(key: StoreKey): Either[StoreError, Long]

trait AsyncObjectReader:
  def read(key: StoreKey, range: ByteRange): Future[Either[StoreError, OwnedBytes]]
  def readAll(key: StoreKey, maxBytes: ByteCount): Future[Either[StoreError, OwnedBytes]]
  def length(key: StoreKey): Future[Either[StoreError, Long]]

/** Optional capability to enumerate all object keys strictly below a path prefix. */
trait ObjectLister:
  def list(prefix: ZarrPath, maxEntries: Int): Either[StoreError, Vector[StoreKey]]

trait AsyncObjectLister:
  def list(prefix: ZarrPath, maxEntries: Int): Future[Either[StoreError, Vector[StoreKey]]]

object AsyncObjectLister:
  def fromSync(lister: ObjectLister)(using scala.concurrent.ExecutionContext): AsyncObjectLister =
    new AsyncObjectLister:
      def list(
          prefix: ZarrPath,
          maxEntries: Int
      ): Future[Either[StoreError, Vector[StoreKey]]] =
        Future(lister.list(prefix, maxEntries))

/** Capability to create one immutable object.
  *
  * A successful call must make the complete object visible atomically. Existing objects are never
  * replaced: implementations return [[StoreError.AlreadyExists]]. Deletion, mutation, listing, and
  * namespace transactions are deliberately not part of this capability.
  */
trait ObjectWriter:
  def create(key: StoreKey, bytes: OwnedBytes): Either[StoreError, Unit]

trait AsyncObjectWriter:
  def create(key: StoreKey, bytes: OwnedBytes): Future[Either[StoreError, Unit]]

enum ObjectWrite(val key: StoreKey, val length: ByteCount):
  case Create(createdKey: StoreKey, createdLength: ByteCount)
      extends ObjectWrite(createdKey, createdLength)

final class MemoryStore private[zarr4s] (
    private val objects: mutable.Map[String, OwnedBytes],
    private val requests: mutable.ArrayBuffer[ObjectRequest],
    private val writes: mutable.ArrayBuffer[ObjectWrite]
) extends ObjectReader,
      ObjectWriter,
      ObjectLister:

  def read(key: StoreKey, range: ByteRange): Either[StoreError, OwnedBytes] =
    requests += ObjectRequest.Range(key, range)
    objects.get(key.value) match
      case None        => Left(StoreError.NotFound(key))
      case Some(bytes) =>
        range.endExclusive match
          case Left(_) => Left(StoreError.RangeNotSatisfiable(key, range, bytes.length.toLong))
          case Right(end) if end > bytes.length.toLong =>
            Left(StoreError.RangeNotSatisfiable(key, range, bytes.length.toLong))
          case Right(end) => Right(bytes.slice(range.offset.toInt, end.toInt))

  def readAll(key: StoreKey, maxBytes: ByteCount): Either[StoreError, OwnedBytes] =
    requests += ObjectRequest.Whole(key)
    objects.get(key.value) match
      case None                                                    => Left(StoreError.NotFound(key))
      case Some(bytes) if bytes.byteCount.toLong > maxBytes.toLong =>
        Left(StoreError.ObjectTooLarge(key, maxBytes.toLong, bytes.byteCount.toLong))
      case Some(bytes) => Right(OwnedBytes.copyOf(bytes.values))

  def length(key: StoreKey): Either[StoreError, Long] =
    requests += ObjectRequest.Length(key)
    objects.get(key.value).map(_.byteCount.toLong).toRight(StoreError.NotFound(key))

  def create(key: StoreKey, bytes: OwnedBytes): Either[StoreError, Unit] =
    objects.synchronized:
      writes += ObjectWrite.Create(key, bytes.byteCount)
      if objects.contains(key.value) then Left(StoreError.AlreadyExists(key))
      else
        objects += key.value -> OwnedBytes.copyOf(bytes.values)
        Right(())

  def list(prefix: ZarrPath, maxEntries: Int): Either[StoreError, Vector[StoreKey]] =
    if maxEntries < 0 then Left(StoreError.ListingTooLarge(prefix, maxEntries))
    else
      val prefixValue = if prefix.value.isEmpty then "" else s"${prefix.value}/"
      val limit = if maxEntries == Int.MaxValue then Int.MaxValue else maxEntries + 1
      val found = objects.keysIterator
        .filter(key => prefixValue.isEmpty && key.nonEmpty || key.startsWith(prefixValue))
        .filter(key => key != prefix.value)
        .take(limit)
        .toVector
        .sorted
      if found.length > maxEntries then Left(StoreError.ListingTooLarge(prefix, maxEntries))
      else Right(found.map(StoreKey.unsafe))

  def trace: Vector[ObjectRequest] = requests.toVector

  def clearTrace(): Unit = requests.clear()

  def writeTrace: Vector[ObjectWrite] = writes.toVector

  def clearWriteTrace(): Unit = writes.clear()

  def snapshot: Map[String, OwnedBytes] =
    objects.iterator.map((key, bytes) => key -> OwnedBytes.copyOf(bytes.values)).toMap

object MemoryStore:
  def apply(objects: Map[String, OwnedBytes]): Either[ZarrError, MemoryStore] =
    val iterator = objects.keysIterator
    while iterator.hasNext do
      StoreKey.from(iterator.next()) match
        case Left(error) => return Left(error)
        case Right(_)    => ()
    val copied = mutable.HashMap.empty[String, OwnedBytes]
    objects.foreach:
      case (key, bytes) => copied += key -> OwnedBytes.copyOf(bytes.values)
    Right(new MemoryStore(copied, mutable.ArrayBuffer.empty, mutable.ArrayBuffer.empty))

final class AsyncMemoryStore private (
    initial: mutable.Map[String, OwnedBytes],
    private val requests: mutable.ArrayBuffer[ObjectRequest],
    private val writes: mutable.ArrayBuffer[ObjectWrite]
) extends AsyncObjectReader,
      AsyncObjectWriter,
      AsyncObjectLister:
  private val sync = new MemoryStore(initial, requests, writes)

  def read(key: StoreKey, range: ByteRange): Future[Either[StoreError, OwnedBytes]] =
    Future.successful(sync.read(key, range))

  def readAll(key: StoreKey, maxBytes: ByteCount): Future[Either[StoreError, OwnedBytes]] =
    Future.successful(sync.readAll(key, maxBytes))

  def length(key: StoreKey): Future[Either[StoreError, Long]] =
    Future.successful(sync.length(key))

  def create(key: StoreKey, bytes: OwnedBytes): Future[Either[StoreError, Unit]] =
    Future.successful(sync.create(key, bytes))

  def list(
      prefix: ZarrPath,
      maxEntries: Int
  ): Future[Either[StoreError, Vector[StoreKey]]] =
    Future.successful(sync.list(prefix, maxEntries))

  def trace: Vector[ObjectRequest] = requests.toVector

  def clearTrace(): Unit = requests.clear()

  def writeTrace: Vector[ObjectWrite] = writes.toVector

  def clearWriteTrace(): Unit = writes.clear()

  def snapshot: Map[String, OwnedBytes] = sync.snapshot

object AsyncMemoryStore:
  def apply(objects: Map[String, OwnedBytes]): Either[ZarrError, AsyncMemoryStore] =
    val iterator = objects.keysIterator
    while iterator.hasNext do
      StoreKey.from(iterator.next()) match
        case Left(error) => return Left(error)
        case Right(_)    => ()
    val copied = mutable.HashMap.empty[String, OwnedBytes]
    objects.foreach:
      case (key, bytes) => copied += key -> OwnedBytes.copyOf(bytes.values)
    Right(
      new AsyncMemoryStore(
        copied,
        mutable.ArrayBuffer.empty,
        mutable.ArrayBuffer.empty
      )
    )

final case class CoalescingLimits(maxGapBytes: Long, maxRangeBytes: ByteCount):
  require(maxGapBytes >= 0L, "maxGapBytes must be non-negative")

object CoalescingLimits:
  val default: CoalescingLimits = CoalescingLimits(4096L, ByteCount.unsafe(8L * 1024L * 1024L))

final case class CoalescedMember[A](value: A, relativeOffset: Int, length: Int)

final case class CoalescedRange[A](range: ByteRange, members: Vector[CoalescedMember[A]])

object RangeCoalescer:
  def coalesce[A](
      ranges: Vector[(ByteRange, A)],
      limits: CoalescingLimits = CoalescingLimits.default
  ): Either[ZarrError, Vector[CoalescedRange[A]]] =
    val sorted = ranges.sortBy(_._1.offset)
    val result = Vector.newBuilder[CoalescedRange[A]]
    var index = 0
    while index < sorted.length do
      val first = sorted(index)
      val start = first._1.offset
      var end = first._1.endExclusive match
        case Left(error)  => return Left(error)
        case Right(found) => found
      val members = Vector.newBuilder[(ByteRange, A)]
      members += first
      index += 1
      var accepting = true
      while index < sorted.length && accepting do
        val next = sorted(index)
        val nextEnd = next._1.endExclusive match
          case Left(error)  => return Left(error)
          case Right(found) => found
        val gap = math.max(0L, next._1.offset - end)
        val mergedEnd = math.max(end, nextEnd)
        if gap <= limits.maxGapBytes && mergedEnd - start <= limits.maxRangeBytes.toLong then
          members += next
          end = mergedEnd
          index += 1
        else accepting = false
      val range = ByteRange(start, end - start) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      val resolved = members
        .result()
        .map: (memberRange, value) =>
          CoalescedMember(
            value,
            (memberRange.offset - start).toInt,
            memberRange.length.toLong.toInt
          )
      result += CoalescedRange(range, resolved)
    Right(result.result())
