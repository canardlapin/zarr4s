package zarr4s

/** An owned typed result from a synchronous or asynchronous array read.
  *
  * The receipt is the same execution accounting value produced by the dynamic reader. `data` owns
  * its primitive storage; callers can request a defensive Scala array with `toArray` without
  * exposing the reader's mutable block.
  */
final case class TypedReadResult[D <: DType](data: DenseArray[D], receipt: ExecutionReceipt):
  def shape: Shape = data.shape

  def values: Array[data.dtype.Element] = data.toArray

/** A dynamically opened array refined once against a compile-time dtype witness. */
final class TypedOpenedArray[D <: DType] private[zarr4s] (
    private[zarr4s] val underlying: OpenedArray,
    val dtype: D
):
  val path: ZarrPath = underlying.path
  val descriptor: ArrayDescriptor = underlying.descriptor
  val format: ZarrFormat = underlying.format

  /** Read the complete logical array without constructing a zero-origin Region. */
  def readAll(limits: ReadLimits = ReadLimits()): Either[ZarrError, TypedReadResult[D]] =
    underlying.readAll(limits).flatMap(TypedReadSupport.materialize(dtype, _))

  def readRegion(
      region: Region,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, TypedReadResult[D]] =
    underlying.readRegion(region, limits).flatMap(TypedReadSupport.materialize(dtype, _))

  def readPoints(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, TypedReadResult[D]] =
    underlying.readPoints(points, limits).flatMap(TypedReadSupport.materialize(dtype, _))

  def read(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  ): Either[ZarrError, TypedReadResult[D]] =
    underlying.read(selection, limits).flatMap(TypedReadSupport.materialize(dtype, _))

  /** Recover the advanced dynamic handle when a caller needs fragment-level operations. */
  def asOpenedArray: OpenedArray = underlying

/** The asynchronous counterpart to [[TypedOpenedArray]]. */
final class AsyncTypedOpenedArray[D <: DType] private[zarr4s] (
    private[zarr4s] val underlying: AsyncOpenedArray,
    val dtype: D
)(using scala.concurrent.ExecutionContext):
  val path: ZarrPath = underlying.path
  val descriptor: ArrayDescriptor = underlying.descriptor
  val format: ZarrFormat = underlying.format

  def readAll(
      limits: ReadLimits = ReadLimits()
  ): scala.concurrent.Future[Either[ZarrError, TypedReadResult[D]]] =
    underlying.readAll(limits).map(_.flatMap(TypedReadSupport.materialize(dtype, _)))

  def readRegion(
      region: Region,
      limits: ReadLimits = ReadLimits()
  ): scala.concurrent.Future[Either[ZarrError, TypedReadResult[D]]] =
    underlying.readRegion(region, limits).map(_.flatMap(TypedReadSupport.materialize(dtype, _)))

  def readPoints(
      points: CoordinateBatch,
      limits: ReadLimits = ReadLimits()
  ): scala.concurrent.Future[Either[ZarrError, TypedReadResult[D]]] =
    underlying.readPoints(points, limits).map(_.flatMap(TypedReadSupport.materialize(dtype, _)))

  def read(
      selection: FactoredSelection,
      limits: ReadLimits = ReadLimits()
  ): scala.concurrent.Future[Either[ZarrError, TypedReadResult[D]]] =
    underlying.read(selection, limits).map(_.flatMap(TypedReadSupport.materialize(dtype, _)))

  def asOpenedArray: AsyncOpenedArray = underlying

private[zarr4s] object TypedReadSupport:
  def refine[D <: DType](opened: OpenedArray, dtype: D): Either[ZarrError, TypedOpenedArray[D]] =
    if matches(opened.descriptor.dataType, dtype) then Right(new TypedOpenedArray(opened, dtype))
    else
      Left(
        ZarrError.DTypeMismatch(
          dtype.name,
          opened.descriptor.dataType.name,
          "opened array"
        )
      )

  def refine[D <: DType](
      opened: AsyncOpenedArray,
      dtype: D
  )(using scala.concurrent.ExecutionContext): Either[ZarrError, AsyncTypedOpenedArray[D]] =
    if matches(opened.descriptor.dataType, dtype) then
      Right(new AsyncTypedOpenedArray(opened, dtype))
    else
      Left(
        ZarrError.DTypeMismatch(
          dtype.name,
          opened.descriptor.dataType.name,
          "opened array"
        )
      )

  def materialize[D <: DType](
      dtype: D,
      result: ReadResult
  ): Either[ZarrError, TypedReadResult[D]] =
    dtype
      .fromBlock(result.block)
      .flatMap: values =>
        DenseArray
          .adopt(dtype, result.shape, values)
          .map(found => TypedReadResult(found, result.receipt))

  private def matches[D <: DType](found: DataTypeCapability, dtype: D): Boolean =
    (found eq dtype.dataType) ||
      (found.name == dtype.name &&
        found.scalarKind == dtype.dataType.scalarKind &&
        found.byteWidth == dtype.dataType.byteWidth)
