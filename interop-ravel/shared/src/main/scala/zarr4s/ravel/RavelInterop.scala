package zarr4s.ravel

import _root_.ravel.{AnyRank, Shape as RavelShape}
import _root_.zarr4s.{Shape as ZarrShape}

private[ravel] object RavelShapeBridge:
  def fromZarr(shape: ZarrShape): Either[RavelInteropError, RavelShape[AnyRank]] =
    val dimensions = new Array[Int](shape.rank.toInt)
    var axis = 0
    while axis < dimensions.length do
      val dimension = shape.axis(axis)
      if dimension > Int.MaxValue.toLong then
        return Left(RavelInteropError.ShapeNotRepresentable(axis, dimension))
      dimensions(axis) = dimension.toInt
      axis += 1

    shape.elementCount match
      case Left(error)                                 => Left(RavelInteropError.Zarr(error))
      case Right(count) if count > Int.MaxValue.toLong =>
        Left(RavelInteropError.ElementCountNotRepresentable(count))
      case Right(_) =>
        RavelShape
          .from(dimensions.toSeq)
          .left
          .map(RavelInteropError.InvalidRavelShape.apply)

  def toZarr(shape: RavelShape[?]): Either[RavelInteropError, ZarrShape] =
    val dimensions = shape.toIArray
    val widened = new Array[Long](dimensions.length)
    var axis = 0
    while axis < dimensions.length do
      widened(axis) = dimensions(axis).toLong
      axis += 1
    ZarrShape.from(widened.toSeq).left.map(RavelInteropError.Zarr.apply)
