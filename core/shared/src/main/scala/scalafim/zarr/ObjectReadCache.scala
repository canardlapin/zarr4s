package scalafim.zarr

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

/** Identity of the immutable store revision represented by one read cache.
  *
  * Callers must choose a new namespace whenever object contents may have
  * changed. The cache deliberately has no implicit or process-global identity.
  */
opaque type CacheNamespace = String

object CacheNamespace:
  def from(value: String): Either[ZarrError, CacheNamespace] =
    if value.isEmpty then
      Left(ZarrError.InvalidSelection("cache namespace must not be empty"))
    else if value != value.trim then
      Left(ZarrError.InvalidSelection("cache namespace must not have surrounding whitespace"))
    else if value.exists(character => Character.isISOControl(character)) then
      Left(ZarrError.InvalidSelection("cache namespace must not contain control characters"))
    else Right(value)

  extension (namespace: CacheNamespace)
    inline def value: String = namespace

final case class CacheLimits(
    maxEntries: Int,
    maxBytes: ByteCount
):
  require(maxEntries >= 0, "maxEntries must be non-negative")

object CacheLimits:
  val default: CacheLimits = CacheLimits(
    maxEntries = 4096,
    maxBytes = ByteCount.unsafe(256L * 1024L * 1024L)
  )

/** Monotone cache counters plus the current resident size. */
final case class CacheStats(
    hits: Long,
    misses: Long,
    downstreamRequests: Long,
    fetchedBytes: Long,
    servedBytes: Long,
    evictedEntries: Long,
    evictedBytes: Long,
    singleFlightJoins: Long,
    residentEntries: Int,
    residentBytes: Long
)

private enum CacheEntryKey:
  case Whole(key: StoreKey)
  case Range(key: StoreKey, offset: Long, length: Long)
  case Length(key: StoreKey)

private enum CacheEntryValue:
  case Bytes(value: OwnedBytes)
  case Length(value: Long)

private final case class CacheEntry(value: CacheEntryValue, weight: Long)

/** A bounded deterministic least-recently-used cache for one immutable revision.
  *
  * Entries are exact whole objects, exact ranges, or object lengths. Range
  * requests may be satisfied from a cached containing range or whole object.
  * Every insertion and return crosses an ownership boundary by copying bytes.
  */
final class ObjectReadCache private (
    val namespace: CacheNamespace,
    val limits: CacheLimits
):
  private val entries = mutable.LinkedHashMap.empty[CacheEntryKey, CacheEntry]
  private var residentBytesValue = 0L
  private var hitsValue = 0L
  private var missesValue = 0L
  private var downstreamRequestsValue = 0L
  private var fetchedBytesValue = 0L
  private var servedBytesValue = 0L
  private var evictedEntriesValue = 0L
  private var evictedBytesValue = 0L
  private var singleFlightJoinsValue = 0L

  def stats: CacheStats = synchronized:
    CacheStats(
      hitsValue,
      missesValue,
      downstreamRequestsValue,
      fetchedBytesValue,
      servedBytesValue,
      evictedEntriesValue,
      evictedBytesValue,
      singleFlightJoinsValue,
      entries.size,
      residentBytesValue
    )

  private[zarr] def cachedRange(
      key: StoreKey,
      requested: ByteRange
  ): Option[OwnedBytes] = synchronized:
    val requestedEnd = requested.endExclusive match
      case Left(_) => return None
      case Right(found) => found
    val exact = CacheEntryKey.Range(key, requested.offset, requested.length.toLong)
    entries.get(exact) match
      case Some(CacheEntry(CacheEntryValue.Bytes(bytes), _)) =>
        touch(exact)
        Some(OwnedBytes.copyOf(bytes.values))
      case _ =>
        var chosen: Option[(CacheEntryKey, Long, OwnedBytes)] = None
        val iterator = entries.iterator
        while iterator.hasNext do
          val (candidateKey, entry) = iterator.next()
          entry.value match
            case CacheEntryValue.Bytes(bytes) => candidateKey match
              case CacheEntryKey.Whole(candidate) if candidate == key &&
                  requestedEnd <= bytes.byteCount.toLong =>
                chosen = chooseContaining(chosen, candidateKey, 0L, bytes)
              case CacheEntryKey.Range(candidate, offset, length) if candidate == key &&
                  offset <= requested.offset && contains(offset, length, requestedEnd) =>
                chosen = chooseContaining(chosen, candidateKey, offset, bytes)
              case _ => ()
            case CacheEntryValue.Length(_) => ()
        chosen.map: (candidateKey, offset, bytes) =>
          val from = (requested.offset - offset).toInt
          val until = from + requested.length.toLong.toInt
          touch(candidateKey)
          bytes.slice(from, until)

  private[zarr] def cachedWhole(key: StoreKey): Option[OwnedBytes] = synchronized:
    val entryKey = CacheEntryKey.Whole(key)
    entries.get(entryKey) match
      case Some(CacheEntry(CacheEntryValue.Bytes(bytes), _)) =>
        touch(entryKey)
        Some(OwnedBytes.copyOf(bytes.values))
      case _ => None

  private[zarr] def cachedLength(key: StoreKey): Option[Long] = synchronized:
    val lengthKey = CacheEntryKey.Length(key)
    entries.get(lengthKey) match
      case Some(CacheEntry(CacheEntryValue.Length(length), _)) =>
        touch(lengthKey)
        Some(length)
      case _ =>
        val wholeKey = CacheEntryKey.Whole(key)
        entries.get(wholeKey) match
          case Some(CacheEntry(CacheEntryValue.Bytes(bytes), _)) =>
            touch(wholeKey)
            Some(bytes.byteCount.toLong)
          case _ => None

  private[zarr] def putRange(
      key: StoreKey,
      range: ByteRange,
      bytes: OwnedBytes
  ): Unit = synchronized:
    if bytes.byteCount == range.length then
      insert(
        CacheEntryKey.Range(key, range.offset, range.length.toLong),
        CacheEntry(CacheEntryValue.Bytes(OwnedBytes.copyOf(bytes.values)), bytes.byteCount.toLong)
      )

  private[zarr] def putWhole(key: StoreKey, bytes: OwnedBytes): Unit = synchronized:
    insert(
      CacheEntryKey.Whole(key),
      CacheEntry(CacheEntryValue.Bytes(OwnedBytes.copyOf(bytes.values)), bytes.byteCount.toLong)
    )

  private[zarr] def putLength(key: StoreKey, length: Long): Unit = synchronized:
    if length >= 0L then
      insert(CacheEntryKey.Length(key), CacheEntry(CacheEntryValue.Length(length), 0L))

  private[zarr] def hit(servedBytes: Long): Unit = synchronized:
    hitsValue = addSaturated(hitsValue, 1L)
    servedBytesValue = addSaturated(servedBytesValue, servedBytes)

  private[zarr] def miss(): Unit = synchronized:
    missesValue = addSaturated(missesValue, 1L)

  private[zarr] def downstream(): Unit = synchronized:
    downstreamRequestsValue = addSaturated(downstreamRequestsValue, 1L)

  private[zarr] def fetched(bytes: Long): Unit = synchronized:
    fetchedBytesValue = addSaturated(fetchedBytesValue, bytes)

  private[zarr] def served(bytes: Long): Unit = synchronized:
    servedBytesValue = addSaturated(servedBytesValue, bytes)

  private[zarr] def joined(): Unit = synchronized:
    singleFlightJoinsValue = addSaturated(singleFlightJoinsValue, 1L)

  private def insert(key: CacheEntryKey, entry: CacheEntry): Unit =
    if limits.maxEntries == 0 || entry.weight > limits.maxBytes.toLong then return
    entries.remove(key).foreach(found => residentBytesValue -= found.weight)
    entries += key -> entry
    residentBytesValue += entry.weight
    while entries.size > limits.maxEntries || residentBytesValue > limits.maxBytes.toLong do
      val (oldestKey, oldest) = entries.head
      entries.remove(oldestKey)
      residentBytesValue -= oldest.weight
      evictedEntriesValue = addSaturated(evictedEntriesValue, 1L)
      evictedBytesValue = addSaturated(evictedBytesValue, oldest.weight)

  private def touch(key: CacheEntryKey): Unit =
    entries.remove(key).foreach(entry => entries += key -> entry)

  private def chooseContaining(
      current: Option[(CacheEntryKey, Long, OwnedBytes)],
      key: CacheEntryKey,
      offset: Long,
      bytes: OwnedBytes
  ): Option[(CacheEntryKey, Long, OwnedBytes)] = current match
    case Some((_, _, found)) if found.byteCount.toLong <= bytes.byteCount.toLong => current
    case _ => Some((key, offset, bytes))

  private def contains(offset: Long, length: Long, requestedEnd: Long): Boolean =
    offset <= Long.MaxValue - length && requestedEnd <= offset + length

  private def addSaturated(left: Long, right: Long): Long =
    if right > Long.MaxValue - left then Long.MaxValue else left + right

object ObjectReadCache:
  def apply(
      namespace: CacheNamespace,
      limits: CacheLimits = CacheLimits.default
  ): ObjectReadCache = new ObjectReadCache(namespace, limits)

/** Synchronous read decorator backed by an explicit revision-scoped cache. */
final class CachingObjectReader(
    store: ObjectReader,
    val cache: ObjectReadCache
) extends ObjectReader:
  def read(key: StoreKey, range: ByteRange): Either[StoreError, OwnedBytes] =
    cache.cachedRange(key, range) match
      case Some(bytes) =>
        cache.hit(bytes.byteCount.toLong)
        Right(bytes)
      case None =>
        cache.miss()
        cache.downstream()
        store.read(key, range).map: bytes =>
          cache.fetched(bytes.byteCount.toLong)
          cache.served(bytes.byteCount.toLong)
          cache.putRange(key, range, bytes)
          OwnedBytes.copyOf(bytes.values)

  def readAll(key: StoreKey, maxBytes: ByteCount): Either[StoreError, OwnedBytes] =
    cache.cachedWhole(key) match
      case Some(bytes) =>
        cache.hit(if bytes.byteCount.toLong <= maxBytes.toLong then bytes.byteCount.toLong else 0L)
        if bytes.byteCount.toLong > maxBytes.toLong then
          Left(StoreError.ObjectTooLarge(key, maxBytes.toLong, bytes.byteCount.toLong))
        else Right(bytes)
      case None =>
        cache.miss()
        cache.downstream()
        store.readAll(key, maxBytes).map: bytes =>
          cache.fetched(bytes.byteCount.toLong)
          cache.served(bytes.byteCount.toLong)
          cache.putWhole(key, bytes)
          OwnedBytes.copyOf(bytes.values)

  def length(key: StoreKey): Either[StoreError, Long] =
    cache.cachedLength(key) match
      case Some(length) =>
        cache.hit(0L)
        Right(length)
      case None =>
        cache.miss()
        cache.downstream()
        store.length(key).map: length =>
          cache.putLength(key, length)
          length

private enum AsyncByteRequest:
  case Range(key: StoreKey, offset: Long, length: Long)
  case Whole(key: StoreKey, maxBytes: Long)

private enum AsyncByteDecision:
  case Cached(result: Either[StoreError, OwnedBytes])
  case Joined(result: Future[Either[StoreError, OwnedBytes]])
  case Start(promise: Promise[Either[StoreError, OwnedBytes]])

private enum AsyncLengthDecision:
  case Cached(value: Long)
  case Joined(result: Future[Either[StoreError, Long]])
  case Start(promise: Promise[Either[StoreError, Long]])

/** Asynchronous read decorator with identical-request single-flight collapse. */
final class CachingAsyncObjectReader(
    store: AsyncObjectReader,
    val cache: ObjectReadCache
)(using executionContext: ExecutionContext) extends AsyncObjectReader:
  private val gate = new AnyRef
  private val byteFlights = mutable.HashMap.empty[
    AsyncByteRequest,
    Future[Either[StoreError, OwnedBytes]]
  ]
  private val lengthFlights = mutable.HashMap.empty[
    StoreKey,
    Future[Either[StoreError, Long]]
  ]

  def read(key: StoreKey, range: ByteRange): Future[Either[StoreError, OwnedBytes]] =
    val request = AsyncByteRequest.Range(key, range.offset, range.length.toLong)
    val decision = gate.synchronized:
      cache.cachedRange(key, range) match
        case Some(bytes) =>
          cache.hit(bytes.byteCount.toLong)
          AsyncByteDecision.Cached(Right(bytes))
        case None => prepareBytes(request)
    executeBytes(
      request,
      decision,
      store.read(key, range),
      bytes => cache.putRange(key, range, bytes)
    )

  def readAll(
      key: StoreKey,
      maxBytes: ByteCount
  ): Future[Either[StoreError, OwnedBytes]] =
    val request = AsyncByteRequest.Whole(key, maxBytes.toLong)
    val decision = gate.synchronized:
      cache.cachedWhole(key) match
        case Some(bytes) =>
          val result =
            if bytes.byteCount.toLong > maxBytes.toLong then
              Left(StoreError.ObjectTooLarge(key, maxBytes.toLong, bytes.byteCount.toLong))
            else Right(bytes)
          cache.hit(result.fold(_ => 0L, _.byteCount.toLong))
          AsyncByteDecision.Cached(result)
        case None => prepareBytes(request)
    executeBytes(
      request,
      decision,
      store.readAll(key, maxBytes),
      bytes => cache.putWhole(key, bytes)
    )

  def length(key: StoreKey): Future[Either[StoreError, Long]] =
    val decision = gate.synchronized:
      cache.cachedLength(key) match
        case Some(value) =>
          cache.hit(0L)
          AsyncLengthDecision.Cached(value)
        case None =>
          cache.miss()
          lengthFlights.get(key) match
            case Some(found) =>
              cache.joined()
              AsyncLengthDecision.Joined(found)
            case None =>
              val promise = Promise[Either[StoreError, Long]]()
              lengthFlights += key -> promise.future
              cache.downstream()
              AsyncLengthDecision.Start(promise)
    decision match
      case AsyncLengthDecision.Cached(value) => Future.successful(Right(value))
      case AsyncLengthDecision.Joined(result) => result
      case AsyncLengthDecision.Start(promise) =>
        safely(store.length(key)).onComplete:
          case Success(result) =>
            gate.synchronized:
              result.foreach(value => cache.putLength(key, value))
              lengthFlights.remove(key)
            promise.success(result)
          case Failure(error) =>
            gate.synchronized(lengthFlights.remove(key))
            promise.failure(error)
        promise.future

  private def prepareBytes(request: AsyncByteRequest): AsyncByteDecision =
    cache.miss()
    byteFlights.get(request) match
      case Some(found) =>
        cache.joined()
        AsyncByteDecision.Joined(found)
      case None =>
        val promise = Promise[Either[StoreError, OwnedBytes]]()
        byteFlights += request -> promise.future
        cache.downstream()
        AsyncByteDecision.Start(promise)

  private def executeBytes(
      request: AsyncByteRequest,
      decision: AsyncByteDecision,
      downstream: => Future[Either[StoreError, OwnedBytes]],
      insert: OwnedBytes => Unit
  ): Future[Either[StoreError, OwnedBytes]] =
    val shared = decision match
      case AsyncByteDecision.Cached(result) => return Future.successful(result)
      case AsyncByteDecision.Joined(result) => result
      case AsyncByteDecision.Start(promise) =>
        safely(downstream).onComplete:
          case Success(result) =>
            gate.synchronized:
              result.foreach: bytes =>
                cache.fetched(bytes.byteCount.toLong)
                insert(bytes)
              byteFlights.remove(request)
            promise.success(result)
          case Failure(error) =>
            gate.synchronized(byteFlights.remove(request))
            promise.failure(error)
        promise.future
    shared.map(_.map: bytes =>
      cache.served(bytes.byteCount.toLong)
      OwnedBytes.copyOf(bytes.values)
    )

  private def safely[A](operation: => Future[A]): Future[A] =
    try operation
    catch case error: Throwable => Future.failed(error)
