package zarr4s

/** A statically named Zarr data type whose element representation is known to Scala.
  *
  * The type member is intentional: `DType.Int16.type` and `DType.UInt16.type` remain distinct
  * type-level witnesses even though both use a compact `Short` array for storage. Custom data types
  * continue to use [[DataTypeCapability]] and the advanced descriptor API until they can provide an
  * honest typed representation.
  */
trait DType:
  type Element

  def name: String
  def dataType: DataTypeCapability

  private[zarr4s] def copyElements(values: Array[Element]): Array[Element]
  private[zarr4s] def toBlock(values: Array[Element]): Either[ZarrError, PrimitiveBlock]
  private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Element]]
  private[zarr4s] def jsonFill(value: Element): JsonValue
  private[zarr4s] def zeroJson: JsonValue

/** Complex values are explicit rather than hidden tuples, keeping the component order visible. */
final case class Complex64Value(real: Float, imaginary: Float)

final case class Complex128Value(real: Double, imaginary: Double)

object DType:
  abstract class Primitive[A](
      val name: String,
      val dataType: DataTypeCapability
  ) extends DType:
    type Element = A

  case object Bool extends Primitive[Boolean]("bool", BuiltInDataTypes.bool):
    private[zarr4s] def copyElements(values: Array[Boolean]): Array[Boolean] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Boolean]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Bool(OwnedBooleans.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Boolean]] =
      block match
        case PrimitiveBlock.Bool(values) => Right(values.toArray)
        case _                           => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Boolean): JsonValue = JsonValue.Bool(value)
    private[zarr4s] val zeroJson: JsonValue = JsonValue.Bool(false)

  case object Int8 extends Primitive[Byte]("int8", BuiltInDataTypes.int8):
    private[zarr4s] def copyElements(values: Array[Byte]): Array[Byte] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Byte]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Int8(OwnedBytes.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Byte]] =
      block match
        case PrimitiveBlock.Int8(values) => Right(values.toArray)
        case _                           => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Byte): JsonValue = number(value.toLong)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object UInt8 extends Primitive[Byte]("uint8", BuiltInDataTypes.uint8):
    private[zarr4s] def copyElements(values: Array[Byte]): Array[Byte] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Byte]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.UInt8(OwnedBytes.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Byte]] =
      block match
        case PrimitiveBlock.UInt8(values) => Right(values.toArray)
        case _                            => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Byte): JsonValue = number(value.toLong & 0xffL)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Int16 extends Primitive[Short]("int16", BuiltInDataTypes.int16):
    private[zarr4s] def copyElements(values: Array[Short]): Array[Short] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Short]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Int16(OwnedShorts.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Short]] =
      block match
        case PrimitiveBlock.Int16(values) => Right(values.toArray)
        case _                            => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Short): JsonValue = number(value.toLong)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object UInt16 extends Primitive[Short]("uint16", BuiltInDataTypes.uint16):
    private[zarr4s] def copyElements(values: Array[Short]): Array[Short] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Short]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.UInt16(OwnedShorts.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Short]] =
      block match
        case PrimitiveBlock.UInt16(values) => Right(values.toArray)
        case _                             => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Short): JsonValue = number(value.toLong & 0xffffL)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Int32 extends Primitive[Int]("int32", BuiltInDataTypes.int32):
    private[zarr4s] def copyElements(values: Array[Int]): Array[Int] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Int]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Int32(OwnedInts.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Int]] =
      block match
        case PrimitiveBlock.Int32(values) => Right(values.toArray)
        case _                            => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Int): JsonValue = number(value.toLong)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object UInt32 extends Primitive[Int]("uint32", BuiltInDataTypes.uint32):
    private[zarr4s] def copyElements(values: Array[Int]): Array[Int] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Int]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.UInt32(OwnedInts.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Int]] =
      block match
        case PrimitiveBlock.UInt32(values) => Right(values.toArray)
        case _                             => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Int): JsonValue = number(value.toLong & 0xffffffffL)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Int64 extends Primitive[Long]("int64", BuiltInDataTypes.int64):
    private[zarr4s] def copyElements(values: Array[Long]): Array[Long] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Long]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Int64(OwnedLongs.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Long]] =
      block match
        case PrimitiveBlock.Int64(values) => Right(values.toArray)
        case _                            => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Long): JsonValue = number(value)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object UInt64 extends Primitive[Long]("uint64", BuiltInDataTypes.uint64):
    private[zarr4s] def copyElements(values: Array[Long]): Array[Long] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Long]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.UInt64(OwnedLongs.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Long]] =
      block match
        case PrimitiveBlock.UInt64(values) => Right(values.toArray)
        case _                             => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Long): JsonValue =
      JsonValue.Num(JsonNumber.unsafe(java.lang.Long.toUnsignedString(value)))

    private[zarr4s] val zeroJson: JsonValue = number(0L)

  /** Float values are converted to exact binary16 bits at the typed boundary. */
  case object Float16 extends Primitive[Float]("float16", BuiltInDataTypes.float16):
    private[zarr4s] def copyElements(values: Array[Float]): Array[Float] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Float]): Either[ZarrError, PrimitiveBlock] =
      Right(
        PrimitiveBlock.Float16(
          OwnedShorts.unsafe(values.map(value => HalfFloat.toBits(value).toShort))
        )
      )

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Float]] =
      block match
        case PrimitiveBlock.Float16(values) =>
          Right(values.toArray.map(value => HalfFloat.fromBits(value & 0xffff)))
        case _ => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Float): JsonValue = floating(value.toDouble)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Float32 extends Primitive[Float]("float32", BuiltInDataTypes.float32):
    private[zarr4s] def copyElements(values: Array[Float]): Array[Float] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Float]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Float32(OwnedFloats.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Float]] =
      block match
        case PrimitiveBlock.Float32(values) => Right(values.toArray)
        case _                              => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Float): JsonValue = floating(value.toDouble)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Float64 extends Primitive[Double]("float64", BuiltInDataTypes.float64):
    private[zarr4s] def copyElements(values: Array[Double]): Array[Double] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(values: Array[Double]): Either[ZarrError, PrimitiveBlock] =
      Right(PrimitiveBlock.Float64(OwnedDoubles.copyOf(values)))

    private[zarr4s] def fromBlock(block: PrimitiveBlock): Either[ZarrError, Array[Double]] =
      block match
        case PrimitiveBlock.Float64(values) => Right(values.toArray)
        case _                              => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Double): JsonValue = floating(value)
    private[zarr4s] val zeroJson: JsonValue = number(0L)

  case object Complex64 extends Primitive[Complex64Value]("complex64", BuiltInDataTypes.complex64):
    private[zarr4s] def copyElements(values: Array[Complex64Value]): Array[Complex64Value] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(
        values: Array[Complex64Value]
    ): Either[ZarrError, PrimitiveBlock] =
      if values.length > Int.MaxValue / 2 then
        Left(
          ZarrError.ResourceLimit(
            "complex64 component allocation",
            Int.MaxValue,
            values.length.toLong * 2L
          )
        )
      else
        val interleaved = new Array[Float](values.length * 2)
        var index = 0
        while index < values.length do
          interleaved(index * 2) = values(index).real
          interleaved(index * 2 + 1) = values(index).imaginary
          index += 1
        Right(PrimitiveBlock.Complex64(OwnedComplex64.unsafe(interleaved)))

    private[zarr4s] def fromBlock(
        block: PrimitiveBlock
    ): Either[ZarrError, Array[Complex64Value]] = block match
      case PrimitiveBlock.Complex64(values) =>
        val result = new Array[Complex64Value](values.length)
        var index = 0
        while index < values.length do
          result(index) = Complex64Value(values.real(index), values.imaginary(index))
          index += 1
        Right(result)
      case _ => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Complex64Value): JsonValue =
      JsonValue.Arr(Vector(floating(value.real.toDouble), floating(value.imaginary.toDouble)))

    private[zarr4s] val zeroJson: JsonValue = JsonValue.Arr(Vector(number(0L), number(0L)))

  case object Complex128
      extends Primitive[Complex128Value]("complex128", BuiltInDataTypes.complex128):
    private[zarr4s] def copyElements(values: Array[Complex128Value]): Array[Complex128Value] =
      java.util.Arrays.copyOf(values, values.length)

    private[zarr4s] def toBlock(
        values: Array[Complex128Value]
    ): Either[ZarrError, PrimitiveBlock] =
      if values.length > Int.MaxValue / 2 then
        Left(
          ZarrError.ResourceLimit(
            "complex128 component allocation",
            Int.MaxValue,
            values.length.toLong * 2L
          )
        )
      else
        val interleaved = new Array[Double](values.length * 2)
        var index = 0
        while index < values.length do
          interleaved(index * 2) = values(index).real
          interleaved(index * 2 + 1) = values(index).imaginary
          index += 1
        Right(PrimitiveBlock.Complex128(OwnedComplex128.unsafe(interleaved)))

    private[zarr4s] def fromBlock(
        block: PrimitiveBlock
    ): Either[ZarrError, Array[Complex128Value]] = block match
      case PrimitiveBlock.Complex128(values) =>
        val result = new Array[Complex128Value](values.length)
        var index = 0
        while index < values.length do
          result(index) = Complex128Value(values.real(index), values.imaginary(index))
          index += 1
        Right(result)
      case _ => Left(mismatch(block))

    private[zarr4s] def jsonFill(value: Complex128Value): JsonValue =
      JsonValue.Arr(Vector(floating(value.real), floating(value.imaginary)))

    private[zarr4s] val zeroJson: JsonValue = JsonValue.Arr(Vector(number(0L), number(0L)))

  val all: Vector[DType] = Vector(
    Bool,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float16,
    Float32,
    Float64,
    Complex64,
    Complex128
  )

  private def mismatch(block: PrimitiveBlock): ZarrError =
    ZarrError.DTypeMismatch("typed dense value", block.toString, "dense storage")

  private def number(value: Long): JsonValue = JsonValue.Num(JsonNumber.unsafe(value.toString))

  private def floating(value: Double): JsonValue =
    if value.isNaN then JsonValue.Str("NaN")
    else if value == Double.PositiveInfinity then JsonValue.Str("Infinity")
    else if value == Double.NegativeInfinity then JsonValue.Str("-Infinity")
    else JsonValue.Num(JsonNumber.unsafe(java.lang.Double.toString(value)))

/** An immutable-shape dense value with owned primitive storage.
  *
  * `copyOf` is the safe public boundary: the input array is copied once. The internal array is
  * never returned directly; `toArray` returns another defensive copy. Use [[DenseArray.adopt]] only
  * when the caller already owns the input array and will not mutate it afterward.
  */
final class DenseArray[D <: DType] private (
    val dtype: D,
    val shape: Shape,
    private[zarr4s] val values: Array[dtype.Element]
):
  def length: Int = values.length

  def elementCount: Long = values.length.toLong

  def toArray: Array[dtype.Element] =
    dtype.copyElements(values)

  def apply(index: Int): dtype.Element = values(index)

  private[zarr4s] def block: Either[ZarrError, PrimitiveBlock] = dtype.toBlock(values)

object DenseArray:
  def copyOf[D <: DType](
      dtype: D,
      shape: Shape,
      values: Array[dtype.Element]
  ): Either[ZarrError, DenseArray[D]] =
    shape.elementCount.flatMap: expected =>
      if expected > Int.MaxValue.toLong then
        Left(ZarrError.ResourceLimit("dense array elements", Int.MaxValue, expected))
      else if expected != values.length.toLong then
        Left(
          ZarrError.InvalidShape(
            s"dense value length ${values.length} does not match shape element count $expected"
          )
        )
      else Right(new DenseArray(dtype, shape, dtype.copyElements(values)))

  /** Adopt an already-owned array without copying it.
    *
    * The caller transfers ownership of `values` and must not mutate it after this call. The dense
    * value still exposes only defensive copies through [[DenseArray.toArray]].
    */
  def adopt[D <: DType](
      dtype: D,
      shape: Shape,
      values: Array[dtype.Element]
  ): Either[ZarrError, DenseArray[D]] =
    shape.elementCount.flatMap: expected =>
      if expected > Int.MaxValue.toLong then
        Left(ZarrError.ResourceLimit("dense array elements", Int.MaxValue, expected))
      else if expected != values.length.toLong then
        Left(
          ZarrError.InvalidShape(
            s"dense value length ${values.length} does not match shape element count $expected"
          )
        )
      else Right(new DenseArray(dtype, shape, values))

/** The checked intent for creating one array through the high-level facade.
  *
  * This first contract deliberately carries only specification-level defaults. Codec and sharding
  * builders will extend it without changing the dtype/shape/ownership boundary.
  */
final class ArraySpec[D <: DType] private (
    val dtype: D,
    val shape: Shape,
    val chunkShape: Shape,
    val fillValue: Option[dtype.Element],
    val dimensionNames: Option[Vector[Option[String]]],
    val attributes: JsonObject,
    val format: ZarrFormat
):
  def withFill(value: dtype.Element): ArraySpec[D] =
    new ArraySpec(dtype, shape, chunkShape, Some(value), dimensionNames, attributes, format)

  def withDimensionNames(names: Vector[Option[String]]): Either[ZarrError, ArraySpec[D]] =
    if names.length != shape.rank.toInt then
      Left(ZarrError.RankMismatch(shape.rank.toInt, names.length, "dimension names"))
    else Right(new ArraySpec(dtype, shape, chunkShape, fillValue, Some(names), attributes, format))

  def withAttributes(found: JsonObject): ArraySpec[D] =
    new ArraySpec(dtype, shape, chunkShape, fillValue, dimensionNames, found, format)

  def asFormat(found: ZarrFormat): ArraySpec[D] =
    new ArraySpec(dtype, shape, chunkShape, fillValue, dimensionNames, attributes, found)

object ArraySpec:
  def apply[D <: DType](
      dtype: D,
      shape: Shape,
      chunkShape: Shape
  ): Either[ZarrError, ArraySpec[D]] =
    validate(dtype, shape, chunkShape, None, None, JsonObject.empty, ZarrFormat.V3)

  def withOptions[D <: DType](
      dtype: D,
      shape: Shape,
      chunkShape: Shape,
      fillValue: Option[dtype.Element],
      dimensionNames: Option[Vector[Option[String]]],
      attributes: JsonObject = JsonObject.empty,
      format: ZarrFormat = ZarrFormat.V3
  ): Either[ZarrError, ArraySpec[D]] =
    validate(dtype, shape, chunkShape, fillValue, dimensionNames, attributes, format)

  private def validate[D <: DType](
      dtype: D,
      shape: Shape,
      chunkShape: Shape,
      fillValue: Option[dtype.Element],
      dimensionNames: Option[Vector[Option[String]]],
      attributes: JsonObject,
      format: ZarrFormat
  ): Either[ZarrError, ArraySpec[D]] =
    for
      _ <- RegularGrid(shape, chunkShape).map(_ => ())
      _ <- dimensionNames match
        case Some(names) if names.length != shape.rank.toInt =>
          Left(ZarrError.RankMismatch(shape.rank.toInt, names.length, "dimension names"))
        case _ => Right(())
    yield new ArraySpec(dtype, shape, chunkShape, fillValue, dimensionNames, attributes, format)
