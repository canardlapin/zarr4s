package zarr4s

private[zarr4s] object LongArrays:
  def copy(values: Array[Long]): Array[Long] =
    java.util.Arrays.copyOf(values, values.length)

  def same(left: Array[Long], right: Array[Long]): Boolean =
    java.util.Arrays.equals(left, right)

  def hash(values: Array[Long]): Int = java.util.Arrays.hashCode(values)

  def render(values: Array[Long]): String = values.mkString("[", ", ", "]")

  def compare(left: Array[Long], right: Array[Long]): Int =
    val common = math.min(left.length, right.length)
    var axis = 0
    while axis < common do
      val comparison = java.lang.Long.compare(left(axis), right(axis))
      if comparison != 0 then return comparison
      axis += 1
    Integer.compare(left.length, right.length)

  def checkedAdd(left: Long, right: Long, context: String): Either[ZarrError, Long] =
    if right > 0L && left > Long.MaxValue - right then Left(ZarrError.ArithmeticOverflow(context))
    else if right < 0L && left < Long.MinValue - right then
      Left(ZarrError.ArithmeticOverflow(context))
    else Right(left + right)

  def checkedMultiply(left: Long, right: Long, context: String): Either[ZarrError, Long] =
    if left == 0L || right == 0L then Right(0L)
    else if left > 0L && right > 0L && left > Long.MaxValue / right then
      Left(ZarrError.ArithmeticOverflow(context))
    else Right(left * right)

opaque type Rank = Int

object Rank:
  def apply(value: Int): Either[ZarrError, Rank] =
    if value >= 0 then Right(value)
    else Left(ZarrError.InvalidShape(s"rank must be non-negative, found $value"))

  private[zarr4s] def unsafe(value: Int): Rank = value

  extension (rank: Rank) inline def toInt: Int = rank

final class Shape private (private[zarr4s] val values: Array[Long]):
  val rank: Rank = Rank.unsafe(values.length)

  def axis(index: Int): Long = values(index)

  def toVector: Vector[Long] = values.toVector

  def elementCount: Either[ZarrError, Long] =
    var axis = 0
    while axis < values.length do
      if values(axis) == 0L then return Right(0L)
      axis += 1

    var product = 1L
    axis = 0
    while axis < values.length do
      LongArrays.checkedMultiply(product, values(axis), "shape element count") match
        case Left(error)  => return Left(error)
        case Right(value) => product = value
      axis += 1
    Right(product)

  override def equals(other: Any): Boolean = other match
    case that: Shape => LongArrays.same(values, that.values)
    case _           => false

  override def hashCode(): Int = LongArrays.hash(values)

  override def toString: String = s"Shape${LongArrays.render(values)}"

object Shape:
  def apply(dimensions: Long*): Either[ZarrError, Shape] = from(dimensions)

  def from(dimensions: Seq[Long]): Either[ZarrError, Shape] =
    val values = dimensions.toArray
    var axis = 0
    while axis < values.length do
      if values(axis) < 0L then
        return Left(
          ZarrError.InvalidShape(
            s"dimension $axis must be non-negative, found ${values(axis)}"
          )
        )
      axis += 1
    Right(unsafe(values))

  private[zarr4s] def unsafe(dimensions: Array[Long]): Shape =
    new Shape(LongArrays.copy(dimensions))

final class Coordinate private (private[zarr4s] val values: Array[Long]):
  val rank: Rank = Rank.unsafe(values.length)

  def axis(index: Int): Long = values(index)

  def toVector: Vector[Long] = values.toVector

  override def equals(other: Any): Boolean = other match
    case that: Coordinate => LongArrays.same(values, that.values)
    case _                => false

  override def hashCode(): Int = LongArrays.hash(values)

  override def toString: String = s"Coordinate${LongArrays.render(values)}"

object Coordinate:
  def apply(indices: Long*): Either[ZarrError, Coordinate] = from(indices)

  def from(indices: Seq[Long]): Either[ZarrError, Coordinate] =
    val values = indices.toArray
    var axis = 0
    while axis < values.length do
      if values(axis) < 0L then
        return Left(
          ZarrError.InvalidCoordinate(
            s"index $axis must be non-negative, found ${values(axis)}"
          )
        )
      axis += 1
    Right(unsafe(values))

  private[zarr4s] def unsafe(indices: Array[Long]): Coordinate =
    new Coordinate(LongArrays.copy(indices))

final case class Region private (origin: Coordinate, extent: Shape):
  val rank: Rank = origin.rank

  def isEmpty: Boolean =
    var axis = 0
    while axis < extent.values.length do
      if extent.values(axis) == 0L then return true
      axis += 1
    false

object Region:
  def within(
      arrayShape: Shape,
      origin: Coordinate,
      extent: Shape
  ): Either[ZarrError, Region] =
    val rank = arrayShape.rank.toInt
    if origin.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, origin.rank.toInt, "region origin"))
    else if extent.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, extent.rank.toInt, "region extent"))
    else
      var axis = 0
      while axis < rank do
        LongArrays.checkedAdd(origin.values(axis), extent.values(axis), s"region axis $axis") match
          case Left(error) => return Left(error)
          case Right(end)  =>
            if end > arrayShape.values(axis) then
              return Left(
                ZarrError.OutOfBounds(
                  s"region ends at $end on axis $axis, whose length is ${arrayShape.values(axis)}"
                )
              )
        axis += 1
      Right(new Region(origin, extent))

final class CoordinateBatch private (
    val shape: Shape,
    val count: Int,
    private[zarr4s] val flattened: Array[Long]
):
  val rank: Rank = shape.rank

  def coordinate(index: Int): Coordinate =
    if index < 0 || index >= count then
      throw new IndexOutOfBoundsException(s"coordinate index $index outside [0, $count)")
    val values = new Array[Long](rank.toInt)
    Array.copy(flattened, index * rank.toInt, values, 0, rank.toInt)
    Coordinate.unsafe(values)

object CoordinateBatch:
  def within(shape: Shape, coordinates: Seq[Coordinate]): Either[ZarrError, CoordinateBatch] =
    val rank = shape.rank.toInt
    if rank != 0 && coordinates.size > Int.MaxValue / rank then
      Left(
        ZarrError.ResourceLimit("coordinate storage", Int.MaxValue, coordinates.size.toLong * rank)
      )
    else
      val flattened = new Array[Long](coordinates.size * rank)
      var point = 0
      while point < coordinates.size do
        val coordinate = coordinates(point)
        if coordinate.rank.toInt != rank then
          return Left(ZarrError.RankMismatch(rank, coordinate.rank.toInt, s"coordinate $point"))
        var axis = 0
        while axis < rank do
          val value = coordinate.values(axis)
          if value >= shape.values(axis) then
            return Left(
              ZarrError.OutOfBounds(
                s"coordinate $point has index $value on axis $axis, whose length is ${shape.values(axis)}"
              )
            )
          flattened(point * rank + axis) = value
          axis += 1
        point += 1
      Right(new CoordinateBatch(shape, coordinates.size, flattened))
