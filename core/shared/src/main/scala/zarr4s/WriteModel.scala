package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

enum ChunkPayload:
  case Fill
  case Values(block: PrimitiveBlock)

trait ChunkProvider:
  /** Returns the stored chunk at a global inner-chunk coordinate.
    *
    * For a direct layout this is the stored chunk coordinate. For a sharded layout it is the global
    * coordinate in the inner-chunk grid. `storedShape` is always the grid's full nominal chunk
    * shape, including at array borders; the provider must fill any overhang with the array's fill
    * value.
    */
  def chunk(
      coordinate: ChunkCoordinate,
      storedShape: Shape
  ): Either[ZarrError, ChunkPayload]

trait AsyncChunkProvider:
  def chunk(
      coordinate: ChunkCoordinate,
      storedShape: Shape
  )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]]

object AsyncChunkProvider:
  def fromSync(provider: ChunkProvider): AsyncChunkProvider = new AsyncChunkProvider:
    def chunk(
        coordinate: ChunkCoordinate,
        storedShape: Shape
    )(using ExecutionContext): Future[Either[ZarrError, ChunkPayload]] =
      Future.successful(provider.chunk(coordinate, storedShape))

final case class WriterLimits(
    maxObjects: Int = 1000000,
    maxChunks: Long = 100000000L,
    maxEncodedChunkBytes: ByteCount = ByteCount.unsafe(512L * 1024L * 1024L),
    maxShardBytes: ByteCount = ByteCount.unsafe(2L * 1024L * 1024L * 1024L),
    maxMetadataBytes: ByteCount = ByteCount.unsafe(16L * 1024L * 1024L),
    maxWrittenBytes: ByteCount = ByteCount.unsafe(Long.MaxValue),
    shardIndex: ShardIndexLimits = ShardIndexLimits()
):
  require(maxObjects >= 0, "maxObjects must be non-negative")
  require(maxChunks >= 0L, "maxChunks must be non-negative")

final case class WrittenObject(
    key: StoreKey,
    length: ByteCount,
    sha256: Sha256Hash
)

/** Durable facts from the objects created so far, including interrupted writes. */
final class WriteProgress private[zarr4s] (
    val objects: Vector[WrittenObject],
    val metadataObjects: Vector[WrittenObject],
    val visitedChunks: Long,
    val encodedChunks: Long,
    val omittedFillChunks: Long,
    val paddingChunks: Long,
    val dataBytes: ByteCount
):
  val createdObjects: Int = objects.length + metadataObjects.length

/** A complete array or group write. Metadata is created last and is therefore the completion marker
  * for store-independent publication.
  */
final class WriteReceipt private[zarr4s] (
    val progress: WriteProgress,
    val metadata: WrittenObject
):
  def objects: Vector[WrittenObject] = progress.objects
  def metadataObjects: Vector[WrittenObject] = progress.metadataObjects
  def metadataSha256: Sha256Hash = metadata.sha256
  def omittedFillChunks: Long = progress.omittedFillChunks
  def visitedChunks: Long = progress.visitedChunks
  def encodedChunks: Long = progress.encodedChunks
  def paddingChunks: Long = progress.paddingChunks
  def dataBytes: ByteCount = progress.dataBytes
  val totalObjects: Int = progress.createdObjects + 1
  val totalBytes: ByteCount = ByteCount.unsafe(
    progress.dataBytes.toLong +
      progress.metadataObjects.map(_.length.toLong).sum +
      metadata.length.toLong
  )

enum WriteOutcome:
  case Complete(receipt: WriteReceipt)
  case Incomplete(progress: WriteProgress, error: ZarrError)

  def toEither: Either[ZarrError, WriteReceipt] = this match
    case Complete(receipt)    => Right(receipt)
    case Incomplete(_, error) => Left(error)

private[zarr4s] final class WriteMetrics:
  private val written = scala.collection.mutable.ArrayBuffer.empty[WrittenObject]
  private val metadata = scala.collection.mutable.ArrayBuffer.empty[WrittenObject]
  private var visited = 0L
  private var encoded = 0L
  private var omitted = 0L
  private var padding = 0L
  private var bytes = 0L
  private var dataBytes = 0L

  def visit(limits: WriterLimits): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(visited, 1L, "visited chunks")
      .flatMap: next =>
        if next > limits.maxChunks then
          Left(ZarrError.ResourceLimit("visited chunks", limits.maxChunks, next))
        else
          visited = next
          Right(())

  def encodedChunk(): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(encoded, 1L, "encoded chunks")
      .map: next =>
        encoded = next

  def omitFill(): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(omitted, 1L, "omitted fill chunks")
      .map: next =>
        omitted = next

  def omitPadding(): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(padding, 1L, "shard padding chunks")
      .map: next =>
        padding = next

  def permitDataObject(
      length: ByteCount,
      metadataLength: ByteCount,
      limits: WriterLimits
  ): Either[ZarrError, Unit] =
    val requestedObjects = written.length.toLong + metadata.length.toLong + 2L
    if requestedObjects > limits.maxObjects.toLong then
      Left(ZarrError.ResourceLimit("written objects", limits.maxObjects, requestedObjects))
    else
      for
        withData <- LongArrays.checkedAdd(bytes, length.toLong, "written bytes")
        total <- LongArrays.checkedAdd(withData, metadataLength.toLong, "written bytes")
        _ <-
          if total <= limits.maxWrittenBytes.toLong then Right(())
          else
            Left(
              ZarrError.ResourceLimit(
                "written bytes",
                limits.maxWrittenBytes.toLong,
                total
              )
            )
      yield ()

  def record(objectValue: WrittenObject): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(bytes, objectValue.length.toLong, "written data bytes")
      .flatMap: next =>
        LongArrays
          .checkedAdd(dataBytes, objectValue.length.toLong, "written data bytes")
          .map: dataNext =>
            written += objectValue
            bytes = next
            dataBytes = dataNext

  def permitMetadataObject(
      length: ByteCount,
      primaryLength: ByteCount,
      limits: WriterLimits
  ): Either[ZarrError, Unit] =
    val requestedObjects = written.length.toLong + metadata.length.toLong + 2L
    if requestedObjects > limits.maxObjects.toLong then
      Left(ZarrError.ResourceLimit("written objects", limits.maxObjects, requestedObjects))
    else
      for
        withPrelude <- LongArrays.checkedAdd(bytes, length.toLong, "written bytes")
        total <- LongArrays.checkedAdd(withPrelude, primaryLength.toLong, "written bytes")
        _ <-
          if total <= limits.maxWrittenBytes.toLong then Right(())
          else
            Left(
              ZarrError.ResourceLimit(
                "written bytes",
                limits.maxWrittenBytes.toLong,
                total
              )
            )
      yield ()

  def recordMetadata(objectValue: WrittenObject): Either[ZarrError, Unit] =
    LongArrays
      .checkedAdd(bytes, objectValue.length.toLong, "written metadata bytes")
      .map: next =>
        metadata += objectValue
        bytes = next

  def snapshot: WriteProgress = new WriteProgress(
    written.toVector,
    metadata.toVector,
    visited,
    encoded,
    omitted,
    padding,
    ByteCount.unsafe(dataBytes)
  )

private[zarr4s] object PrimitiveBlockType:
  def validate(
      block: PrimitiveBlock,
      dataType: DataTypeCapability,
      expectedElements: Long
  ): Either[ZarrError, Unit] =
    val matches = dataType.scalarKind.accepts(block)
    if !matches then
      Left(
        ZarrError.InvalidSelection(
          s"chunk block type does not match ${dataType.name}"
        )
      )
    else if block.elementCount.toLong != expectedElements then
      Left(
        ZarrError.InvalidSelection(
          s"chunk block has ${block.elementCount} elements, expected $expectedElements"
        )
      )
    else Right(())
