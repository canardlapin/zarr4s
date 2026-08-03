package zarr4s.ravel

import _root_.ravel.AnyNDArray
import _root_.zarr4s.*
import scala.concurrent.{ExecutionContext, Future}

/** An owned canonical Ravel array paired with the original zarr4s execution receipt. */
final case class RavelReadResult[D <: DType](
    data: AnyNDArray[RavelValue[D]],
    receipt: ExecutionReceipt
):
  def shape: _root_.ravel.Shape[_root_.ravel.AnyRank] = data.shape

private[ravel] object RavelReadSupport:
  def materialize[D <: DType](
      mapping: RavelElement[D],
      result: ReadResult
  ): Either[RavelInteropError, RavelReadResult[D]] =
    mapping
      .materialize(result.block, result.shape)
      .map(data => RavelReadResult[D](data, result.receipt))

extension [D <: DType](opened: TypedOpenedArray[D])
  /** Read the complete logical Zarr array directly into one owned canonical Ravel array. */
  def readAllNDArray(
      limits: ReadLimits = ReadLimits()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, RavelReadResult[D]] =
    opened.asOpenedArray
      .readAll(limits)
      .left
      .map(RavelInteropError.Zarr.apply)
      .flatMap:
        RavelReadSupport.materialize(mapping, _)

  def readRegionNDArray(
      region: Region,
      limits: ReadLimits = ReadLimits()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, RavelReadResult[D]] =
    opened.asOpenedArray
      .readRegion(region, limits)
      .left
      .map(RavelInteropError.Zarr.apply)
      .flatMap:
        RavelReadSupport.materialize(mapping, _)

  def readPointsNDArray(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, RavelReadResult[D]] =
    opened.asOpenedArray
      .readPoints(points, limits)
      .left
      .map(RavelInteropError.Zarr.apply)
      .flatMap:
        RavelReadSupport.materialize(mapping, _)

  def readNDArray(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, RavelReadResult[D]] =
    opened.asOpenedArray
      .read(selection, limits)
      .left
      .map(RavelInteropError.Zarr.apply)
      .flatMap:
        RavelReadSupport.materialize(mapping, _)

extension [D <: DType](opened: AsyncTypedOpenedArray[D])
  def readAllNDArrayAsync(
      limits: ReadLimits = ReadLimits()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, RavelReadResult[D]]] =
    opened.asOpenedArray
      .readAll(limits)
      .map:
        _.left.map(RavelInteropError.Zarr.apply).flatMap(RavelReadSupport.materialize(mapping, _))

  def readRegionNDArrayAsync(
      region: Region,
      limits: ReadLimits = ReadLimits()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, RavelReadResult[D]]] =
    opened.asOpenedArray
      .readRegion(region, limits)
      .map:
        _.left.map(RavelInteropError.Zarr.apply).flatMap(RavelReadSupport.materialize(mapping, _))

  def readPointsNDArrayAsync(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, RavelReadResult[D]]] =
    opened.asOpenedArray
      .readPoints(points, limits)
      .map:
        _.left.map(RavelInteropError.Zarr.apply).flatMap(RavelReadSupport.materialize(mapping, _))

  def readNDArrayAsync(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, RavelReadResult[D]]] =
    opened.asOpenedArray
      .read(selection, limits)
      .map:
        _.left.map(RavelInteropError.Zarr.apply).flatMap(RavelReadSupport.materialize(mapping, _))
