package zarr4s

final class AxisIndices private (private[zarr4s] val values: Array[Long]):
  def length: Int = values.length
  def apply(index: Int): Long = values(index)
  def toVector: Vector[Long] = values.toVector

  override def equals(other: Any): Boolean = other match
    case that: AxisIndices => LongArrays.same(values, that.values)
    case _                 => false

  override def hashCode(): Int = LongArrays.hash(values)

object AxisIndices:
  def from(values: Seq[Long]): Either[ZarrError, AxisIndices] =
    val copied = values.toArray
    var index = 0
    while index < copied.length do
      if copied(index) < 0L then
        return Left(
          ZarrError.InvalidSelection(
            s"axis index $index must be non-negative, found ${copied(index)}"
          )
        )
      index += 1
    Right(new AxisIndices(copied))

  private[zarr4s] def unsafe(values: Array[Long]): AxisIndices =
    new AxisIndices(LongArrays.copy(values))

final class AxisSlice private (
    val start: Long,
    val stopExclusive: Long,
    val step: Long
):
  override def equals(other: Any): Boolean = other match
    case that: AxisSlice =>
      start == that.start && stopExclusive == that.stopExclusive && step == that.step
    case _ => false

  override def hashCode(): Int = (start, stopExclusive, step).hashCode

  override def toString: String = s"AxisSlice($start,$stopExclusive,$step)"

object AxisSlice:
  def apply(
      start: Long,
      stopExclusive: Long,
      step: Long = 1L
  ): Either[ZarrError, AxisSlice] =
    if start < 0L then
      Left(ZarrError.InvalidSelection(s"slice start must be non-negative, found $start"))
    else if stopExclusive < 0L then
      Left(
        ZarrError.InvalidSelection(
          s"slice stop must be non-negative, found $stopExclusive"
        )
      )
    else if step <= 0L then
      Left(
        ZarrError.InvalidSelection(
          s"slice step must be positive, found $step; descending slices are not supported"
        )
      )
    else if stopExclusive < start then
      Left(
        ZarrError.InvalidSelection(
          s"slice stop $stopExclusive precedes start $start; descending slices are not supported"
        )
      )
    else Right(new AxisSlice(start, stopExclusive, step))

enum AxisSelector:
  case All
  case Slice(value: AxisSlice)
  case Indices(value: AxisIndices)

object AxisSelector:
  def slice(
      start: Long,
      stopExclusive: Long,
      step: Long = 1L
  ): Either[ZarrError, AxisSelector] =
    AxisSlice(start, stopExclusive, step).map(AxisSelector.Slice.apply)

  def indices(values: Long*): Either[ZarrError, AxisSelector] =
    AxisIndices.from(values).map(AxisSelector.Indices.apply)

private[zarr4s] enum BoundAxisSelection:
  case Strided(start: Long, step: Long, length: Long)
  case Gather(indices: AxisIndices)

  def outputLength: Long = this match
    case Strided(_, _, length) => length
    case Gather(indices)       => indices.length.toLong

final class FactoredSelection private (
    val arrayShape: Shape,
    val selectors: Vector[AxisSelector],
    private[zarr4s] val axes: Vector[BoundAxisSelection],
    val outputShape: Shape,
    val requestedElements: Long
)

object FactoredSelection:
  def apply(
      shape: Shape,
      selectors: AxisSelector*
  ): Either[ZarrError, FactoredSelection] = within(shape, selectors)

  def within(
      shape: Shape,
      selectors: Seq[AxisSelector]
  ): Either[ZarrError, FactoredSelection] =
    val copied = selectors.toVector
    val rank = shape.rank.toInt
    if copied.length != rank then
      Left(ZarrError.RankMismatch(rank, copied.length, "factored selection"))
    else
      val bound = Vector.newBuilder[BoundAxisSelection]
      val output = new Array[Long](rank)
      var axis = 0
      while axis < rank do
        val dimension = shape.axis(axis)
        copied(axis) match
          case AxisSelector.All =>
            bound += BoundAxisSelection.Strided(0L, 1L, dimension)
            output(axis) = dimension
          case AxisSelector.Slice(slice) =>
            if slice.start > dimension || slice.stopExclusive > dimension then
              return Left(
                ZarrError.OutOfBounds(
                  s"slice [${slice.start}, ${slice.stopExclusive}) exceeds axis $axis length $dimension"
                )
              )
            val length =
              if slice.start == slice.stopExclusive then 0L
              else 1L + (slice.stopExclusive - slice.start - 1L) / slice.step
            bound += BoundAxisSelection.Strided(slice.start, slice.step, length)
            output(axis) = length
          case AxisSelector.Indices(indices) =>
            var index = 0
            while index < indices.length do
              val value = indices(index)
              if value >= dimension then
                return Left(
                  ZarrError.OutOfBounds(
                    s"axis $axis index $index has value $value outside length $dimension"
                  )
                )
              index += 1
            bound += BoundAxisSelection.Gather(indices)
            output(axis) = indices.length.toLong
        axis += 1
      val outputShape = Shape.unsafe(output)
      outputShape.elementCount.map: elements =>
        new FactoredSelection(shape, copied, bound.result(), outputShape, elements)

  def all(shape: Shape): FactoredSelection =
    within(shape, Vector.fill(shape.rank.toInt)(AxisSelector.All)) match
      case Right(found) => found
      case Left(error)  => throw IllegalStateException(error.message)

private[zarr4s] final class LongOffsets private (private[zarr4s] val values: Array[Long]):
  def length: Int = values.length
  def apply(index: Int): Long = values(index)

private[zarr4s] object LongOffsets:
  def unsafe(values: Array[Long]): LongOffsets = new LongOffsets(LongArrays.copy(values))

private[zarr4s] enum IndexRun:
  case Affine(start: Long, step: Long, count: Long)
  case Explicit(values: LongOffsets)

  def length: Long = this match
    case Affine(_, _, found) => found
    case Explicit(values)    => values.length.toLong

  def apply(index: Long): Long = this match
    case Affine(start, step, _) => start + index * step
    case Explicit(values)       => values(index.toInt)

private[zarr4s] final case class AxisProjection(source: IndexRun, destination: IndexRun):
  require(source.length == destination.length, "source and destination axis projections must align")

  def length: Long = source.length

private[zarr4s] final case class FactoredChunkCopy(
    axes: Vector[AxisProjection],
    fragmentShape: Shape,
    elementCount: Long
)
