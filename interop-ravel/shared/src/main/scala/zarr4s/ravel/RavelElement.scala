package zarr4s.ravel

import _root_.zarr4s.*
import _root_.ravel.{
  AnyNDArray,
  AnyRank,
  ArrayBuilder,
  CanonicalArray,
  DType as RavelDType,
  NDArray,
  UInt8 as RavelUInt8,
  UInt16 as RavelUInt16
}
import _root_.ravel.CanonicalArray.*
import scala.util.control.NonFatal

/** The exact Ravel element representation of a statically known Zarr dtype. */
type RavelValue[D <: _root_.zarr4s.DType] = D match
  case _root_.zarr4s.DType.Bool.type    => Boolean
  case _root_.zarr4s.DType.Int8.type    => Byte
  case _root_.zarr4s.DType.UInt8.type   => RavelUInt8
  case _root_.zarr4s.DType.Int16.type   => Short
  case _root_.zarr4s.DType.UInt16.type  => RavelUInt16
  case _root_.zarr4s.DType.Int32.type   => Int
  case _root_.zarr4s.DType.Int64.type   => Long
  case _root_.zarr4s.DType.Float32.type => Float
  case _root_.zarr4s.DType.Float64.type => Double

/** Evidence that one Zarr dtype has an exact, non-widening Ravel representation. */
sealed trait RavelElement[D <: _root_.zarr4s.DType]:
  def ravelDType: RavelDType[RavelValue[D]]

  private[ravel] def materialize(
      block: PrimitiveBlock,
      shape: _root_.zarr4s.Shape
  ): Either[RavelInteropError, AnyNDArray[RavelValue[D]]]

  private[ravel] def copyValue[R <: AnyRank](
      source: CanonicalArray[RavelValue[D], R],
      sourceIndex: Int,
      target: PrimitiveBlock,
      targetIndex: Int
  ): Option[RavelInteropError]

/** Existential evidence returned when a runtime Zarr dtype has an exact Ravel representation. */
sealed trait SomeRavelElement:
  type D <: _root_.zarr4s.DType
  val dtype: D
  val mapping: RavelElement[D]

object SomeRavelElement:
  private[ravel] def apply[D0 <: _root_.zarr4s.DType](
      found: D0,
      foundMapping: RavelElement[D0]
  ): SomeRavelElement = new SomeRavelElement:
    type D = D0
    val dtype: D0 = found
    val mapping: RavelElement[D0] = foundMapping

object RavelElement:
  /** Refine a runtime dtype without name-based or widening dispatch. */
  def exact(dtype: _root_.zarr4s.DType): Either[RavelInteropError, SomeRavelElement] = dtype match
    case DType.Bool    => Right(SomeRavelElement(DType.Bool, bool))
    case DType.Int8    => Right(SomeRavelElement(DType.Int8, int8))
    case DType.UInt8   => Right(SomeRavelElement(DType.UInt8, uint8))
    case DType.Int16   => Right(SomeRavelElement(DType.Int16, int16))
    case DType.UInt16  => Right(SomeRavelElement(DType.UInt16, uint16))
    case DType.Int32   => Right(SomeRavelElement(DType.Int32, int32))
    case DType.Int64   => Right(SomeRavelElement(DType.Int64, int64))
    case DType.Float32 => Right(SomeRavelElement(DType.Float32, float32))
    case DType.Float64 => Right(SomeRavelElement(DType.Float64, float64))
    case other         => Left(RavelInteropError.UnsupportedDType(other.name))

  private def build[A](
      shape: _root_.zarr4s.Shape,
      dtype: RavelDType[A]
  )(body: ArrayBuilder[A] => Unit): Either[RavelInteropError, AnyNDArray[A]] =
    RavelShapeBridge
      .fromZarr(shape)
      .flatMap: targetShape =>
        try Right(NDArray.build[A, AnyRank](targetShape)(body)(using dtype))
        catch
          case NonFatal(error) =>
            Left(
              RavelInteropError.MaterializationFailure(
                Option(error.getMessage).getOrElse(error.toString)
              )
            )

  private def mismatch(expected: String, block: PrimitiveBlock): RavelInteropError =
    RavelInteropError.Zarr(
      ZarrError.DTypeMismatch(expected, block.toString, "Ravel materialization")
    )

  given bool: RavelElement[_root_.zarr4s.DType.Bool.type] with
    val ravelDType = summon[RavelDType[Boolean]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Bool(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("bool", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Boolean, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Bool(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("bool", other))

  given int8: RavelElement[_root_.zarr4s.DType.Int8.type] with
    val ravelDType = summon[RavelDType[Byte]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Int8(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("int8", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Byte, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Int8(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("int8", other))

  given uint8: RavelElement[_root_.zarr4s.DType.UInt8.type] with
    val ravelDType = summon[RavelDType[RavelUInt8]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.UInt8(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, RavelUInt8.unsafe(values(index).toInt & 0xff))
            index += 1
      case other => Left(mismatch("uint8", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[RavelUInt8, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.UInt8(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex).toInt.toByte
        None
      case other => Some(mismatch("uint8", other))

  given int16: RavelElement[_root_.zarr4s.DType.Int16.type] with
    val ravelDType = summon[RavelDType[Short]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Int16(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("int16", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Short, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Int16(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("int16", other))

  given uint16: RavelElement[_root_.zarr4s.DType.UInt16.type] with
    val ravelDType = summon[RavelDType[RavelUInt16]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.UInt16(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, RavelUInt16.unsafe(values(index).toInt & 0xffff))
            index += 1
      case other => Left(mismatch("uint16", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[RavelUInt16, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.UInt16(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex).toInt.toShort
        None
      case other => Some(mismatch("uint16", other))

  given int32: RavelElement[_root_.zarr4s.DType.Int32.type] with
    val ravelDType = summon[RavelDType[Int]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Int32(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("int32", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Int, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Int32(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("int32", other))

  given int64: RavelElement[_root_.zarr4s.DType.Int64.type] with
    val ravelDType = summon[RavelDType[Long]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Int64(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("int64", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Long, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Int64(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("int64", other))

  given float32: RavelElement[_root_.zarr4s.DType.Float32.type] with
    val ravelDType = summon[RavelDType[Float]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Float32(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("float32", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Float, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Float32(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("float32", other))

  given float64: RavelElement[_root_.zarr4s.DType.Float64.type] with
    val ravelDType = summon[RavelDType[Double]]

    private[ravel] def materialize(block: PrimitiveBlock, shape: _root_.zarr4s.Shape) = block match
      case PrimitiveBlock.Float64(values) =>
        build(shape, ravelDType): target =>
          var index = 0
          while index < values.length do
            target.writeLinear(index, values(index))
            index += 1
      case other => Left(mismatch("float64", other))

    private[ravel] def copyValue[R <: AnyRank](
        source: CanonicalArray[Double, R],
        sourceIndex: Int,
        target: PrimitiveBlock,
        targetIndex: Int
    ) = target match
      case PrimitiveBlock.Float64(values) =>
        values.values(targetIndex) = source.readLinear(sourceIndex)
        None
      case other => Some(mismatch("float64", other))
