package zarr4s.ravel

import _root_.zarr4s.ZarrError
import _root_.ravel.{InvalidShape, NonContiguousLayout}

/** Failures introduced by the boundary between Zarr's Long-shaped storage model and Ravel's
  * portable Int-buffer model.
  *
  * Native Zarr failures remain intact in [[RavelInteropError.Zarr]]. They are not copied into or
  * hidden behind a second Zarr error algebra.
  */
enum RavelInteropError:
  case Zarr(error: ZarrError)
  case ShapeNotRepresentable(axis: Int, dimension: Long)
  case ElementCountNotRepresentable(elementCount: Long)
  case InvalidRavelShape(error: InvalidShape)
  case NonCanonicalInput(error: NonContiguousLayout)
  case MaterializationFailure(details: String)
  case UnsupportedDType(dtype: String)

  def message: String = this match
    case Zarr(error)                            => error.message
    case ShapeNotRepresentable(axis, dimension) =>
      s"Zarr dimension $dimension on axis $axis exceeds Ravel's portable Int dimension limit"
    case ElementCountNotRepresentable(elementCount) =>
      s"Zarr element count $elementCount exceeds Ravel's portable Int buffer limit"
    case InvalidRavelShape(error)        => error.getMessage
    case NonCanonicalInput(error)        => error.getMessage
    case MaterializationFailure(details) => s"Ravel materialization failed: $details"
    case UnsupportedDType(dtype)         => s"Zarr dtype $dtype has no exact Ravel representation"
