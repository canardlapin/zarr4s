package scalafim.zarr

final class FragmentAxis private[zarr] (private val run: IndexRun):
  def length: Long = run.length

  def outputIndex(position: Long): Either[ZarrError, Long] =
    if position < 0L || position >= run.length then
      Left(ZarrError.OutOfBounds(
        s"fragment axis position $position outside length ${run.length}"
      ))
    else Right(run(position))

  def toVector: Either[ZarrError, Vector[Long]] =
    if length > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("fragment axis materialization", Int.MaxValue, length))
    else Right(Vector.tabulate(length.toInt)(index => run(index.toLong)))

  private[zarr] def unsafeOutputIndex(position: Long): Long = run(position)

final class FragmentPlacement private[zarr] (
    val outputShape: Shape,
    private val axes: Vector[FragmentAxis]
):
  val rank: Rank = outputShape.rank

  def axis(index: Int): Either[ZarrError, FragmentAxis] =
    if index < 0 || index >= axes.length then
      Left(ZarrError.OutOfBounds(s"fragment axis $index outside rank ${axes.length}"))
    else Right(axes(index))

  def axisVector: Vector[FragmentAxis] = axes

  private[zarr] def unsafeOutputIndex(axis: Int, position: Long): Long =
    axes(axis).unsafeOutputIndex(position)

private[zarr] object FragmentPlacement:
  def apply(outputShape: Shape, copy: FactoredChunkCopy): FragmentPlacement =
    new FragmentPlacement(
      outputShape,
      copy.axes.map(projection => new FragmentAxis(projection.destination))
    )

enum FragmentSource:
  case Decoded
  case Fill

final class ChunkFragment private[zarr] (
    val coordinate: ChunkCoordinate,
    val values: PrimitiveBlock,
    val shape: Shape,
    val placement: FragmentPlacement,
    val source: FragmentSource
):
  val elementCount: Int = values.elementCount

private[zarr] object ChunkFragment:
  def decoded(
      coordinate: ChunkCoordinate,
      decoded: PrimitiveBlock,
      decodedShape: Shape,
      copy: FactoredChunkCopy,
      outputShape: Shape,
      dataType: DataTypeCapability,
      fill: StoredScalar
  ): Either[ZarrError, ChunkFragment] =
    PrimitiveBlockBuilder.compact(dataType, fill, decoded, decodedShape, copy).map: compacted =>
      new ChunkFragment(
        coordinate,
        compacted,
        copy.fragmentShape,
        FragmentPlacement(outputShape, copy),
        FragmentSource.Decoded
      )

  def fill(
      coordinate: ChunkCoordinate,
      copy: FactoredChunkCopy,
      outputShape: Shape,
      dataType: DataTypeCapability,
      fill: StoredScalar
  ): Either[ZarrError, ChunkFragment] =
    PrimitiveBlockBuilder(dataType, fill, copy.fragmentShape).map: builder =>
      new ChunkFragment(
        coordinate,
        builder.result(),
        copy.fragmentShape,
        FragmentPlacement(outputShape, copy),
        FragmentSource.Fill
      )

enum FragmentControl[+S]:
  case Continue(state: S)
  case Stop(state: S)

final class FragmentReceipt private[zarr] (
    val objectRequests: Int,
    val rangeRequests: Int,
    val lengthRequests: Int,
    val bytesRead: Long,
    val indexBytesRead: Long,
    val dataBytesRead: Long,
    val plannedChunks: Int,
    val visitedChunks: Int,
    val decodedChunks: Int,
    val fillChunks: Int,
    val emittedFragments: Int,
    val emittedElements: Long,
    val requestedElements: Long,
    val elementByteWidth: Int,
    val plannedShards: Int,
    val completed: Boolean
):
  require(objectRequests >= 0, "objectRequests must be non-negative")
  require(rangeRequests >= 0, "rangeRequests must be non-negative")
  require(lengthRequests >= 0, "lengthRequests must be non-negative")
  require(bytesRead >= 0L, "bytesRead must be non-negative")
  require(indexBytesRead >= 0L, "indexBytesRead must be non-negative")
  require(dataBytesRead >= 0L, "dataBytesRead must be non-negative")
  require(indexBytesRead + dataBytesRead == bytesRead, "index and data bytes must account for bytesRead")
  require(plannedChunks >= 0, "plannedChunks must be non-negative")
  require(visitedChunks >= 0 && visitedChunks <= plannedChunks, "visitedChunks must be within the plan")
  require(decodedChunks >= 0, "decodedChunks must be non-negative")
  require(fillChunks >= 0, "fillChunks must be non-negative")
  require(decodedChunks + fillChunks == visitedChunks, "decoded and fill chunks must account for visits")
  require(emittedFragments == visitedChunks, "one fragment must be emitted per visited chunk")
  require(emittedElements >= 0L && emittedElements <= requestedElements, "emitted elements must be within request")
  require(elementByteWidth > 0, "elementByteWidth must be positive")
  require(plannedShards >= 0, "plannedShards must be non-negative")
  require(!completed || visitedChunks == plannedChunks, "completed receipt must visit every planned chunk")
  require(!completed || emittedElements == requestedElements, "completed receipt must emit every requested element")

  val requestedLogicalBytes: Long = requestedElements * elementByteWidth.toLong
  val emittedLogicalBytes: Long = emittedElements * elementByteWidth.toLong

  def readAmplification: Double =
    if emittedLogicalBytes == 0L then 0.0
    else bytesRead.toDouble / emittedLogicalBytes.toDouble

final case class FragmentFoldResult[S](state: S, receipt: FragmentReceipt)

private[zarr] final class FragmentMetrics private (
    val plannedChunks: Int,
    val requestedElements: Long,
    val elementByteWidth: Int,
    val plannedShards: Int
):
  var objectRequests = 0
  var rangeRequests = 0
  var lengthRequests = 0
  var indexBytesRead = 0L
  var dataBytesRead = 0L
  var visitedChunks = 0
  var decodedChunks = 0
  var fillChunks = 0
  var emittedElements = 0L

  def decoded(fragment: ChunkFragment): Unit =
    visitedChunks += 1
    decodedChunks += 1
    emittedElements += fragment.elementCount.toLong

  def fill(fragment: ChunkFragment): Unit =
    visitedChunks += 1
    fillChunks += 1
    emittedElements += fragment.elementCount.toLong

  def result(completed: Boolean): FragmentReceipt = new FragmentReceipt(
    objectRequests,
    rangeRequests,
    lengthRequests,
    indexBytesRead + dataBytesRead,
    indexBytesRead,
    dataBytesRead,
    plannedChunks,
    visitedChunks,
    decodedChunks,
    fillChunks,
    visitedChunks,
    emittedElements,
    requestedElements,
    elementByteWidth,
    plannedShards,
    completed
  )

private[zarr] object FragmentMetrics:
  def checked(
      plannedChunks: Int,
      requestedElements: Long,
      elementByteWidth: Int,
      plannedShards: Int = 0
  ): Either[ZarrError, FragmentMetrics] =
    LongArrays.checkedMultiply(
      requestedElements,
      elementByteWidth.toLong,
      "fragment requested logical bytes"
    ).map: _ =>
      new FragmentMetrics(plannedChunks, requestedElements, elementByteWidth, plannedShards)
