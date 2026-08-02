package zarr4s

final case class ArrayCodecResult(block: PrimitiveBlock, shape: Shape)

/** An executable array-to-array codec with an explicit shape law.
  *
  * The decoded shape is the stable reference point for both directions. This lets a runtime derive
  * the encoded shape before deserializing bytes and then invert the same stage without guessing at
  * dimension order.
  */
trait ExecutableArrayCodec extends CompiledCodec:
  final val input = CodecRepresentation.ArrayValues
  final val output = CodecRepresentation.ArrayValues

  def encodedShape(decodedShape: Shape): Either[ZarrError, Shape]

  /** Data type visible to the following array or bytes codec. */
  def encodedDataType(
      decodedDataType: DataTypeCapability
  ): Either[ZarrError, DataTypeCapability] = Right(decodedDataType)

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
  val configuration: JsonObject = JsonObject.unsafe(
    Vector(
      "order" -> JsonValue.Arr(order.map: axis =>
        JsonValue.Num(JsonNumber.unsafe(axis.toString)))
    )
  )

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
    case _                    => false

  override def hashCode(): Int = order.hashCode

  override def toString: String = s"TransposeCodec(${order.mkString("[", ",", "]")})"

  private def transpose(
      input: PrimitiveBlock,
      inputShape: Shape,
      permutation: Vector[Int]
  ): Either[ZarrError, (PrimitiveBlock, Shape)] =
    val outputShape = Shape.from(permutation.map(inputShape.axis)) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    val count = inputShape.elementCount match
      case Left(error)  => return Left(error)
      case Right(found) => found
    if count > Int.MaxValue.toLong then
      Left(ZarrError.ResourceLimit("transposed elements", Int.MaxValue, count))
    else if input.elementCount.toLong != count then
      Left(
        ZarrError.InvalidSelection(
          s"transpose block has ${input.elementCount} elements, expected $count"
        )
      )
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

/** Numcodecs-compatible adjacent-difference filter for fixed-width numeric arrays. */
final class DeltaCodec private (
    val dtype: String,
    val astype: Option[String],
    private val decodedType: DataTypeCapability,
    val encodedType: DataTypeCapability,
    val encodedEndianness: Option[Endianness]
) extends ExecutableArrayCodec:
  val name = "delta"
  val configuration: JsonObject = JsonObject.unsafe(
    Vector(
      "dtype" -> JsonValue.Str(dtype)
    ) ++ astype.map(value => "astype" -> JsonValue.Str(value)).toVector
  )

  override def encodedShape(decodedShape: Shape): Either[ZarrError, Shape] = Right(decodedShape)

  override def encodedDataType(
      decodedDataType: DataTypeCapability
  ): Either[ZarrError, DataTypeCapability] =
    if decodedDataType.scalarKind == decodedType.scalarKind then Right(encodedType)
    else
      Left(
        ZarrError.InvalidCodecChain(
          s"delta dtype ${decodedType.name} does not match ${decodedDataType.name}"
        )
      )

  def encodeArray(
      decoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, ArrayCodecResult] =
    validate(decoded, decodedShape, decodedType.scalarKind).flatMap: _ =>
      DeltaCodec.family(decodedType.scalarKind, encodedType.scalarKind) match
        case DeltaCodec.Family.Boolean =>
          DeltaCodec
            .booleanValues(decoded)
            .map: input =>
              ArrayCodecResult(
                PrimitiveBlock.Bool(OwnedBooleans.unsafe(DeltaCodec.encodeBooleans(input))),
                decodedShape
              )
        case DeltaCodec.Family.Integral =>
          for
            input <- DeltaCodec.integralValues(decoded, decodedType.scalarKind)
            output <- DeltaCodec.encodeIntegral(
              input,
              decodedType.scalarKind,
              encodedType.scalarKind
            )
          yield ArrayCodecResult(
            DeltaCodec.integralBlock(encodedType.scalarKind, output),
            decodedShape
          )
        case DeltaCodec.Family.Floating =>
          for
            input <- DeltaCodec.floatingValues(decoded, decodedType.scalarKind)
            output <- DeltaCodec.encodeFloating(
              input,
              decodedType.scalarKind,
              encodedType.scalarKind
            )
          yield ArrayCodecResult(
            DeltaCodec.floatingBlock(encodedType.scalarKind, output),
            decodedShape
          )
        case DeltaCodec.Family.Unsupported =>
          Left(ZarrError.UnsupportedDataType(s"delta dtype ${decodedType.name}"))

  def decodeArray(
      encoded: PrimitiveBlock,
      decodedShape: Shape
  ): Either[ZarrError, PrimitiveBlock] =
    validate(encoded, decodedShape, encodedType.scalarKind).flatMap: _ =>
      DeltaCodec.family(decodedType.scalarKind, encodedType.scalarKind) match
        case DeltaCodec.Family.Boolean =>
          DeltaCodec
            .booleanValues(encoded)
            .map(DeltaCodec.decodeBooleans)
            .map(values => PrimitiveBlock.Bool(OwnedBooleans.unsafe(values)))
        case DeltaCodec.Family.Integral =>
          DeltaCodec
            .integralValues(encoded, encodedType.scalarKind)
            .flatMap(values =>
              DeltaCodec
                .decodeIntegral(values, decodedType.scalarKind)
                .map(DeltaCodec.integralBlock(decodedType.scalarKind, _))
            )
        case DeltaCodec.Family.Floating =>
          DeltaCodec
            .floatingValues(encoded, encodedType.scalarKind)
            .flatMap(values =>
              DeltaCodec
                .decodeFloating(values, decodedType.scalarKind)
                .map(DeltaCodec.floatingBlock(decodedType.scalarKind, _))
            )
        case DeltaCodec.Family.Unsupported =>
          Left(ZarrError.UnsupportedDataType(s"delta dtype ${decodedType.name}"))

  override def equals(other: Any): Boolean = other match
    case that: DeltaCodec => dtype == that.dtype && astype == that.astype
    case _                => false

  override def hashCode(): Int = 31 * dtype.hashCode + astype.hashCode

  override def toString: String =
    s"DeltaCodec(dtype='$dtype',astype=${astype.getOrElse(dtype)})"

  private def validate(
      block: PrimitiveBlock,
      shape: Shape,
      kind: ScalarKind
  ): Either[ZarrError, Unit] =
    shape.elementCount.flatMap: count =>
      if count > Int.MaxValue.toLong then
        Left(ZarrError.ResourceLimit("delta elements", Int.MaxValue, count))
      else if block.elementCount.toLong != count then
        Left(
          ZarrError.InvalidSelection(
            s"delta block has ${block.elementCount} elements, expected $count"
          )
        )
      else if !kind.accepts(block) then
        Left(ZarrError.InvalidSelection(s"delta block type does not match ${kind.toString}"))
      else Right(())

object DeltaCodec:
  private enum Family:
    case Boolean
    case Integral
    case Floating
    case Unsupported

  private[zarr4s] def fromConfiguration(
      configuration: JsonObject,
      dataType: DataTypeCapability
  ): Either[String, DeltaCodec] =
    for
      dtypeName <- requiredString(configuration, "dtype")
      (decodedType, decodedEndianness) <- BuiltInDataTypes.fromV2DType(dtypeName)
      _ <-
        if decodedType.scalarKind == dataType.scalarKind then Right(())
        else Left(s"delta dtype $dtypeName does not match array data type ${dataType.name}")
      astypeValue <- configuration.get("astype") match
        case None                       => Right(None)
        case Some(JsonValue.Str(found)) => Right(Some(found))
        case Some(_)                    => Left("delta astype must be a string")
      parsedAstype <- astypeValue match
        case None        => Right((decodedType, decodedEndianness))
        case Some(found) => BuiltInDataTypes.fromV2DType(found)
      (encodedType, encodedEndianness) = parsedAstype
      _ <- family(decodedType.scalarKind, encodedType.scalarKind) match
        case Family.Unsupported =>
          Left("delta dtype and astype must both be boolean, integral, or floating")
        case _ => Right(())
    yield new DeltaCodec(
      dtypeName,
      astypeValue,
      dataType,
      if astypeValue.isEmpty then dataType else encodedType,
      encodedEndianness
    )

  private def requiredString(configuration: JsonObject, field: String): Either[String, String] =
    configuration.get(field) match
      case Some(JsonValue.Str(found)) => Right(found)
      case Some(_)                    => Left(s"delta $field must be a string")
      case None                       => Left(s"delta $field is required")

  private def family(decoded: ScalarKind, encoded: ScalarKind): Family =
    if decoded == ScalarKind.Bool && encoded == ScalarKind.Bool then Family.Boolean
    else if isIntegral(decoded) && isIntegral(encoded) then Family.Integral
    else if isFloating(decoded) && isFloating(encoded) then Family.Floating
    else Family.Unsupported

  private def isIntegral(kind: ScalarKind): Boolean = kind match
    case ScalarKind.Signed8 | ScalarKind.Unsigned8 | ScalarKind.Signed16 | ScalarKind.Unsigned16 |
        ScalarKind.Signed32 | ScalarKind.Unsigned32 | ScalarKind.Signed64 | ScalarKind.Unsigned64 =>
      true
    case _ => false

  private def isFloating(kind: ScalarKind): Boolean = kind match
    case ScalarKind.Float16 | ScalarKind.Float32 | ScalarKind.Float64 => true
    case _                                                            => false

  private def booleanValues(block: PrimitiveBlock): Either[ZarrError, Array[Boolean]] = block match
    case PrimitiveBlock.Bool(values) => Right(values.values.clone())
    case _ => Left(ZarrError.InvalidSelection("delta boolean block mismatch"))

  private def encodeBooleans(values: Array[Boolean]): Array[Boolean] =
    val output = new Array[Boolean](values.length)
    if values.nonEmpty then
      output(0) = values(0)
      var index = 1
      while index < values.length do
        output(index) = values(index) != values(index - 1)
        index += 1
    output

  private def decodeBooleans(values: Array[Boolean]): Array[Boolean] =
    val output = new Array[Boolean](values.length)
    if values.nonEmpty then
      output(0) = values(0)
      var index = 1
      while index < values.length do
        output(index) = output(index - 1) != values(index)
        index += 1
    output

  private def integralValues(
      block: PrimitiveBlock,
      kind: ScalarKind
  ): Either[ZarrError, Array[Long]] = (kind, block) match
    case (ScalarKind.Signed8, PrimitiveBlock.Int8(values))    => Right(values.values.map(_.toLong))
    case (ScalarKind.Unsigned8, PrimitiveBlock.UInt8(values)) =>
      Right(values.values.map(value => (value & 0xff).toLong))
    case (ScalarKind.Signed16, PrimitiveBlock.Int16(values)) => Right(values.values.map(_.toLong))
    case (ScalarKind.Unsigned16, PrimitiveBlock.UInt16(values)) =>
      Right(values.values.map(value => (value & 0xffff).toLong))
    case (ScalarKind.Signed32, PrimitiveBlock.Int32(values)) => Right(values.values.map(_.toLong))
    case (ScalarKind.Unsigned32, PrimitiveBlock.UInt32(values)) =>
      Right(values.values.map(value => Integer.toUnsignedLong(value)))
    case (ScalarKind.Signed64, PrimitiveBlock.Int64(values))    => Right(values.values.clone())
    case (ScalarKind.Unsigned64, PrimitiveBlock.UInt64(values)) => Right(values.values.clone())
    case _ => Left(ZarrError.InvalidSelection("delta integral block mismatch"))

  private def encodeIntegral(
      values: Array[Long],
      decodedKind: ScalarKind,
      encodedKind: ScalarKind
  ): Either[ZarrError, Array[Long]] =
    val output = new Array[Long](values.length)
    if values.nonEmpty then
      output(0) = wrap(values(0), encodedKind)
      var index = 1
      while index < values.length do
        output(index) = wrap(
          wrap(values(index) - values(index - 1), decodedKind),
          encodedKind
        )
        index += 1
    Right(output)

  private def decodeIntegral(
      values: Array[Long],
      decodedKind: ScalarKind
  ): Either[ZarrError, Array[Long]] =
    val output = new Array[Long](values.length)
    if values.nonEmpty then
      output(0) = wrap(values(0), decodedKind)
      var index = 1
      while index < values.length do
        output(index) = wrap(output(index - 1) + values(index), decodedKind)
        index += 1
    Right(output)

  private def wrap(value: Long, kind: ScalarKind): Long =
    val bits = kind.byteWidth * 8
    if bits == 64 then value
    else
      val modulus = 1L << bits
      val normalized = ((value % modulus) + modulus) % modulus
      kind match
        case ScalarKind.Signed8 | ScalarKind.Signed16 | ScalarKind.Signed32
            if normalized >= modulus / 2L =>
          normalized - modulus
        case _ => normalized

  private[zarr4s] def integralBlock(kind: ScalarKind, values: Array[Long]): PrimitiveBlock =
    kind match
      case ScalarKind.Signed8    => PrimitiveBlock.Int8(OwnedBytes.unsafe(values.map(_.toByte)))
      case ScalarKind.Unsigned8  => PrimitiveBlock.UInt8(OwnedBytes.unsafe(values.map(_.toByte)))
      case ScalarKind.Signed16   => PrimitiveBlock.Int16(OwnedShorts.unsafe(values.map(_.toShort)))
      case ScalarKind.Unsigned16 => PrimitiveBlock.UInt16(OwnedShorts.unsafe(values.map(_.toShort)))
      case ScalarKind.Signed32   => PrimitiveBlock.Int32(OwnedInts.unsafe(values.map(_.toInt)))
      case ScalarKind.Unsigned32 => PrimitiveBlock.UInt32(OwnedInts.unsafe(values.map(_.toInt)))
      case ScalarKind.Signed64   => PrimitiveBlock.Int64(OwnedLongs.unsafe(values.clone()))
      case ScalarKind.Unsigned64 => PrimitiveBlock.UInt64(OwnedLongs.unsafe(values.clone()))
      case _ => throw new IllegalArgumentException(s"not an integral scalar kind: $kind")

  private def floatingValues(
      block: PrimitiveBlock,
      kind: ScalarKind
  ): Either[ZarrError, Array[Double]] = (kind, block) match
    case (ScalarKind.Float16, PrimitiveBlock.Float16(values)) =>
      Right(values.values.map(value => HalfFloat.fromBits(value & 0xffff).toDouble))
    case (ScalarKind.Float32, PrimitiveBlock.Float32(values)) =>
      Right(values.values.map(_.toDouble))
    case (ScalarKind.Float64, PrimitiveBlock.Float64(values)) => Right(values.values.clone())
    case _ => Left(ZarrError.InvalidSelection("delta floating block mismatch"))

  private def encodeFloating(
      values: Array[Double],
      decodedKind: ScalarKind,
      encodedKind: ScalarKind
  ): Either[ZarrError, Array[Double]] =
    val output = new Array[Double](values.length)
    if values.nonEmpty then
      output(0) = quantize(values(0), encodedKind)
      var index = 1
      while index < values.length do
        output(index) = quantize(
          quantize(values(index) - values(index - 1), decodedKind),
          encodedKind
        )
        index += 1
    Right(output)

  private def decodeFloating(
      values: Array[Double],
      decodedKind: ScalarKind
  ): Either[ZarrError, Array[Double]] =
    val output = new Array[Double](values.length)
    if values.nonEmpty then
      output(0) = quantize(values(0), decodedKind)
      var index = 1
      while index < values.length do
        output(index) = quantize(output(index - 1) + values(index), decodedKind)
        index += 1
    Right(output)

  private def quantize(value: Double, kind: ScalarKind): Double = kind match
    case ScalarKind.Float16 => HalfFloat.fromBits(HalfFloat.toBits(value.toFloat)).toDouble
    case ScalarKind.Float32 => value.toFloat.toDouble
    case ScalarKind.Float64 => value
    case _                  => value

  private[zarr4s] def floatingBlock(kind: ScalarKind, values: Array[Double]): PrimitiveBlock =
    kind match
      case ScalarKind.Float16 =>
        PrimitiveBlock.Float16(
          OwnedShorts.unsafe(values.map(value => HalfFloat.toBits(value.toFloat).toShort))
        )
      case ScalarKind.Float32 => PrimitiveBlock.Float32(OwnedFloats.unsafe(values.map(_.toFloat)))
      case ScalarKind.Float64 => PrimitiveBlock.Float64(OwnedDoubles.unsafe(values.clone()))
      case _ => throw new IllegalArgumentException(s"not a floating scalar kind: $kind")
