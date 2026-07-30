package zarr4s

final class ChunkCoordinate private[zarr4s] (private[zarr4s] val values: Array[Long]):
  val rank: Rank = Rank.unsafe(values.length)

  def axis(index: Int): Long = values(index)

  def toVector: Vector[Long] = values.toVector

  override def equals(other: Any): Boolean = other match
    case that: ChunkCoordinate => LongArrays.same(values, that.values)
    case _                     => false

  override def hashCode(): Int = LongArrays.hash(values)

  override def toString: String = s"ChunkCoordinate${LongArrays.render(values)}"

object ChunkCoordinate:
  private[zarr4s] def unsafe(values: Array[Long]): ChunkCoordinate =
    new ChunkCoordinate(LongArrays.copy(values))

final class RegularGrid private (
    val arrayShape: Shape,
    val chunkShape: Shape,
    val gridShape: Shape
):
  val rank: Rank = arrayShape.rank

  def chunkOrigin(coordinate: ChunkCoordinate): Either[ZarrError, Coordinate] =
    if coordinate.rank.toInt != rank.toInt then
      Left(ZarrError.RankMismatch(rank.toInt, coordinate.rank.toInt, "chunk coordinate"))
    else
      val origin = new Array[Long](rank.toInt)
      var axis = 0
      while axis < rank.toInt do
        if coordinate.values(axis) < 0L || coordinate.values(axis) >= gridShape.values(axis) then
          return Left(
            ZarrError.OutOfBounds(
              s"chunk index ${coordinate.values(axis)} on axis $axis outside grid length ${gridShape.values(axis)}"
            )
          )
        LongArrays.checkedMultiply(
          coordinate.values(axis),
          chunkShape.values(axis),
          s"chunk origin axis $axis"
        ) match
          case Left(error)  => return Left(error)
          case Right(value) => origin(axis) = value
        axis += 1
      Right(Coordinate.unsafe(origin))

object RegularGrid:
  def apply(arrayShape: Shape, chunkShape: Shape): Either[ZarrError, RegularGrid] =
    val rank = arrayShape.rank.toInt
    if chunkShape.rank.toInt != rank then
      Left(ZarrError.RankMismatch(rank, chunkShape.rank.toInt, "chunk shape"))
    else
      val grid = new Array[Long](rank)
      var axis = 0
      while axis < rank do
        val chunk = chunkShape.values(axis)
        if chunk <= 0L then
          return Left(
            ZarrError.InvalidGrid(
              s"chunk dimension $axis must be positive, found $chunk"
            )
          )
        val size = arrayShape.values(axis)
        grid(axis) = if size == 0L then 0L else 1L + (size - 1L) / chunk
        axis += 1
      Right(new RegularGrid(arrayShape, chunkShape, Shape.unsafe(grid)))

enum ChunkSeparator:
  case Slash
  case Dot

opaque type StoreKey = String

object StoreKey:
  def from(value: String): Either[ZarrError, StoreKey] =
    validate(value).map(_ => value)

  private[zarr4s] def unsafe(value: String): StoreKey = value

  extension (key: StoreKey) inline def value: String = key

  private def validate(value: String): Either[ZarrError, Unit] =
    val lower = value.toLowerCase
    if value.isEmpty then Left(ZarrError.InvalidSelection("store key must not be empty"))
    else if value.startsWith("/") || value.startsWith("\\") then
      Left(ZarrError.InvalidSelection("store key must be relative"))
    else if value.contains('\\') then
      Left(ZarrError.InvalidSelection("store key must use '/' separators"))
    else if lower.contains("%2f") || lower.contains("%5c") then
      Left(ZarrError.InvalidSelection("store key must not contain encoded separators"))
    else if !value.forall(isPortableKeyCharacter) then
      Left(ZarrError.InvalidSelection("store key contains a non-portable character"))
    else
      val segments = value.split("/", -1)
      if segments.exists(segment => segment.isEmpty || segment == "." || segment == "..") then
        Left(ZarrError.InvalidSelection("store key contains an empty or traversal segment"))
      else Right(())

  private def isPortableKeyCharacter(character: Char): Boolean =
    character >= 'a' && character <= 'z' ||
      character >= 'A' && character <= 'Z' ||
      character >= '0' && character <= '9' ||
      character == '-' || character == '_' || character == '.' ||
      character == '~' || character == '/'

opaque type ZarrPath = String

object ZarrPath:
  val root: ZarrPath = ""

  def apply(value: String): Either[ZarrError, ZarrPath] =
    if value.isEmpty then Right(root)
    else StoreKey.from(value).map(_.value)

  private[zarr4s] def unsafe(value: String): ZarrPath = value

  extension (path: ZarrPath)
    inline def value: String = path

    def resolve(relative: String): Either[ZarrError, ZarrPath] =
      if relative.isEmpty then Right(path)
      else
        val combined = if path.isEmpty then relative else s"$path/$relative"
        ZarrPath(combined)

    def key(child: String): Either[ZarrError, StoreKey] =
      val combined = if path.isEmpty then child else s"$path/$child"
      StoreKey.from(combined)

trait ChunkKeyEncoding:
  def name: String
  def separator: ChunkSeparator
  def encode(coordinate: ChunkCoordinate): StoreKey

final case class DefaultChunkKeyEncoding(separator: ChunkSeparator) extends ChunkKeyEncoding:
  val name = "default"

  def encode(coordinate: ChunkCoordinate): StoreKey =
    if coordinate.rank.toInt == 0 then StoreKey.unsafe("c")
    else
      val delimiter = separator match
        case ChunkSeparator.Slash => "/"
        case ChunkSeparator.Dot   => "."
      StoreKey.unsafe(coordinate.values.mkString(s"c$delimiter", delimiter, ""))

final case class V2ChunkKeyEncoding(separator: ChunkSeparator) extends ChunkKeyEncoding:
  val name = "v2"

  def encode(coordinate: ChunkCoordinate): StoreKey =
    if coordinate.rank.toInt == 0 then StoreKey.unsafe("0")
    else
      val delimiter = separator match
        case ChunkSeparator.Slash => "/"
        case ChunkSeparator.Dot   => "."
      StoreKey.unsafe(coordinate.values.mkString(delimiter))
