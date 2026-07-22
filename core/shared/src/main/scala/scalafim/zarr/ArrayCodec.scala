package scalafim.zarr

final case class ArrayCodecResult(block: PrimitiveBlock, shape: Shape)

/** An executable array-to-array codec with an explicit shape law.
  *
  * The decoded shape is the stable reference point for both directions. This
  * lets a runtime derive the encoded shape before deserializing bytes and then
  * invert the same stage without guessing at dimension order.
  */
trait ExecutableArrayCodec extends CompiledCodec:
  final val input = CodecRepresentation.ArrayValues
  final val output = CodecRepresentation.ArrayValues

  def encodedShape(decodedShape: Shape): Either[ZarrError, Shape]

  def encodeArray(
      decoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, ArrayCodecResult]

  def decodeArray(
      encoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, PrimitiveBlock]

final class TransposeCodec private (val order: Vector[Int]) extends ExecutableArrayCodec:
  val name = "transpose"
  val configuration: JsonObject = JsonObject.unsafe(Vector(
    "order" -> JsonValue.Arr(order.map: axis =>
      JsonValue.Num(JsonNumber.unsafe(axis.toString))
    )
  ))

  private val inverseOrder: Vector[Int] =
    val result = new Array[Int](order.length)
    var axis = 0
    while axis < order.length do
      result(order(axis)) = axis
      axis += 1
    result.toVector

  def encodedShape(decodedShape: Shape): Either[ZarrError, Shape] =
    if decodedShape.rank.toInt != order.length then
      Left(ZarrError.RankMismatch(order.length, decodedShape.rank.toInt, "transpose order"))
    else Shape.from(order.map(decodedShape.axis))

  def encodeArray(
      decoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, ArrayCodecResult] =
    transpose(decoded, decodedShape, order).map: (block, shape) =>
      ArrayCodecResult(block, shape)

  def decodeArray(
      encoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, PrimitiveBlock] =
    encodedShape(decodedShape).flatMap: shape =>
      transpose(encoded, shape, inverseOrder).flatMap: (block, foundShape) =>
        if foundShape == decodedShape then Right(block)
        else Left(ZarrError.InvalidCodecChain("transpose inverse produced the wrong shape"))

  override def equals(other: Any): Boolean = other match
    case that: TransposeCodec => order == that.order
    case _ => false

  override def hashCode(): Int = order.hashCode

  override def toString: String = s"TransposeCodec(${order.mkString("[", ",", "]")})"

  private def transpose(
      input: PrimitiveBlock,
      inputShape: Shape,
      permutation: Vector[Int]
  ): Either[ZarrError, (PrimitiveBlock, Shape)] =
    val outputShape = Shape.from(permutation.map(inputShape.axis)) match
      case Left(error) => return Left(error)
      case Right(found) => found
    val count = inputShape.elementCount match
      case Left(error) => return Left(error)
      case Right(found) => found
    if count > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("transposed elements", Int.MaxValue, count))
    else if input.elementCount.toLong != count then
      Left(ZarrError.InvalidSelection(
        s"transpose block has ${input.elementCount} elements, expected $count"
      ))
    else
      val sourceIndices = new Array[Int](count.toInt)
      val sourceCoordinate = new Array[Long](inputShape.rank.toInt)
      var destinationOffset = 0
      while destinationOffset < sourceIndices.length do
        var remainder = destinationOffset.toLong
        var outputAxis = outputShape.rank.toInt - 1
        while outputAxis >= 0 do
          val dimension = outputShape.axis(outputAxis)
          val coordinate = remainder % dimension
          remainder /= dimension
          sourceCoordinate(permutation(outputAxis)) = coordinate
          outputAxis -= 1
        var sourceOffset = 0L
        var sourceAxis = 0
        while sourceAxis < sourceCoordinate.length do
          sourceOffset = sourceOffset * inputShape.axis(sourceAxis) + sourceCoordinate(sourceAxis)
          sourceAxis += 1
        sourceIndices(destinationOffset) = sourceOffset.toInt
        destinationOffset += 1
      Right(input.reordered(sourceIndices) -> outputShape)

object TransposeCodec:
  def from(order: Seq[Int]): Either[String, TransposeCodec] =
    val copied = order.toVector
    val seen = new Array[Boolean](copied.length)
    var axis = 0
    while axis < copied.length do
      val found = copied(axis)
      if found < 0 || found >= copied.length then
        return Left(s"transpose order must be a permutation of [0, ${copied.length}), found $found")
      if seen(found) then return Left(s"transpose order repeats axis $found")
      seen(found) = true
      axis += 1
    Right(new TransposeCodec(copied))
