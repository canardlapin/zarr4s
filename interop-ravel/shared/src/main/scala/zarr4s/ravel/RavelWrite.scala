package zarr4s.ravel

import _root_.ravel.{AnyRank, CanonicalArray, NDArray}
import _root_.zarr4s.*
import scala.concurrent.{ExecutionContext, Future}

/** An immutable, whole-buffer Ravel array accepted as a Zarr write source.
  *
  * Construct this with [[RavelArraySource.fromCanonical]] to transfer no storage and with
  * [[RavelArraySource.copyOf]] when a view must first be materialized in logical C-order.
  */
final class RavelArraySource[D <: DType, R <: AnyRank] private (
    val dtype: D,
    val shape: _root_.zarr4s.Shape,
    private[ravel] val array: CanonicalArray[RavelValue[D], R]
):
  def elementCount: Int = array.size

  /** Adapt this source to zarr4s's typed provider boundary after checking descriptor identity. */
  def typedProvider(
      descriptor: ArrayDescriptor
  )(using mapping: RavelElement[D]): Either[RavelInteropError, TypedChunkProvider[D]] =
    if descriptor.shape != shape then
      Left(
        RavelInteropError.Zarr(
          ZarrError.InvalidShape(
            s"Ravel source shape $shape does not match descriptor shape ${descriptor.shape}"
          )
        )
      )
    else if descriptor.dataType ne dtype.dataType then
      Left(
        RavelInteropError.Zarr(
          ZarrError.DTypeMismatch(descriptor.dataType.name, dtype.name, "Ravel chunk provider")
        )
      )
    else
      Right(
        TypedChunkProvider.from(
          dtype,
          new RavelChunkProvider[D, R](descriptor, this, mapping)
        )
      )

object RavelArraySource:
  /** Refine an immutable owned Ravel array without copying its storage. */
  def fromCanonical[D <: DType & Singleton, A, R <: AnyRank](
      dtype: D,
      array: NDArray[A, R]
  )(using
      mapping: RavelElement[D],
      elementType: A =:= RavelValue[D]
  ): Either[RavelInteropError, RavelArraySource[D, R]] =
    val _ = mapping
    val typedArray = elementType.substituteCo[[Element] =>> NDArray[Element, R]](array)
    CanonicalArray
      .from(typedArray)
      .left
      .map(RavelInteropError.NonCanonicalInput.apply)
      .flatMap: canonical =>
        RavelShapeBridge
          .toZarr(typedArray.shape)
          .map(shape => new RavelArraySource(dtype, shape, canonical))

  /** Copy any immutable Ravel view into a whole canonical owned source in logical C-order. */
  def copyOf[D <: DType & Singleton, A, R <: AnyRank](
      dtype: D,
      array: NDArray[A, R]
  )(using
      mapping: RavelElement[D],
      elementType: A =:= RavelValue[D]
  ): Either[RavelInteropError, RavelArraySource[D, R]] =
    fromCanonical(dtype, array.copy)(using mapping, elementType)

private final class RavelChunkProvider[D <: DType, R <: AnyRank](
    descriptor: ArrayDescriptor,
    source: RavelArraySource[D, R],
    mapping: RavelElement[D]
) extends ChunkProvider:
  def chunk(
      coordinate: ChunkCoordinate,
      storedShape: _root_.zarr4s.Shape
  ): Either[ZarrError, ChunkPayload] =
    val grid = ChunkProvider.logicalGrid(descriptor)
    val prepared = for
      _ <- validateCoordinate(grid, coordinate)
      _ <-
        if storedShape == grid.chunkShape then Right(())
        else
          Left(
            RavelInteropError.Zarr(
              ZarrError.InvalidGrid(
                s"Ravel provider expected stored shape ${grid.chunkShape}, found $storedShape"
              )
            )
          )
      count <- storedShape.elementCount.left.map(RavelInteropError.Zarr.apply)
      _ <-
        if count <= Int.MaxValue.toLong then Right(())
        else Left(RavelInteropError.ElementCountNotRepresentable(count))
      target <- descriptor.dataType.scalarKind
        .allocate(descriptor.fillValue, count.toInt, descriptor.dataType.name)
        .left
        .map(RavelInteropError.Zarr.apply)
      origin <- chunkOrigin(grid.chunkShape, coordinate)
      extent = logicalExtent(source.shape, origin, grid.chunkShape)
      _ <- copyLogicalIntersection(origin, extent, storedShape, target)
    yield ChunkPayload.Values(target)
    prepared.left.map:
      case RavelInteropError.Zarr(error) => error
      case error                         => ZarrError.WriteFailure(error.message)

  private def validateCoordinate(
      grid: RegularGrid,
      coordinate: ChunkCoordinate
  ): Either[RavelInteropError, Unit] =
    if coordinate.rank.toInt != grid.rank.toInt then
      Left(
        RavelInteropError.Zarr(
          ZarrError.RankMismatch(grid.rank.toInt, coordinate.rank.toInt, "Ravel chunk coordinate")
        )
      )
    else
      var axis = 0
      while axis < grid.rank.toInt do
        val value = coordinate.axis(axis)
        if value < 0L || value >= grid.gridShape.axis(axis) then
          return Left(
            RavelInteropError.Zarr(
              ZarrError.OutOfBounds(
                s"chunk index $value on axis $axis outside grid length ${grid.gridShape.axis(axis)}"
              )
            )
          )
        axis += 1
      Right(())

  private def chunkOrigin(
      chunkShape: _root_.zarr4s.Shape,
      coordinate: ChunkCoordinate
  ): Either[RavelInteropError, Array[Long]] =
    val origin = new Array[Long](chunkShape.rank.toInt)
    var axis = 0
    while axis < origin.length do
      LongArrays.checkedMultiply(
        coordinate.axis(axis),
        chunkShape.axis(axis),
        s"Ravel chunk origin axis $axis"
      ) match
        case Left(error)  => return Left(RavelInteropError.Zarr(error))
        case Right(value) => origin(axis) = value
      axis += 1
    Right(origin)

  private def logicalExtent(
      arrayShape: _root_.zarr4s.Shape,
      origin: Array[Long],
      chunkShape: _root_.zarr4s.Shape
  ): Array[Long] =
    val extent = new Array[Long](origin.length)
    var axis = 0
    while axis < extent.length do
      extent(axis) = math.max(
        0L,
        math.min(chunkShape.axis(axis), arrayShape.axis(axis) - origin(axis))
      )
      axis += 1
    extent

  private def copyLogicalIntersection(
      origin: Array[Long],
      extent: Array[Long],
      storedShape: _root_.zarr4s.Shape,
      target: PrimitiveBlock
  ): Either[RavelInteropError, Unit] =
    var count = 1L
    var axis = 0
    while axis < extent.length do
      if extent(axis) == 0L then count = 0L
      else if count != 0L then count *= extent(axis)
      axis += 1

    val cursor = new Array[Long](extent.length)
    var copied = 0L
    while copied < count do
      var sourceIndex = 0L
      var targetIndex = 0L
      axis = 0
      while axis < extent.length do
        sourceIndex = sourceIndex * source.shape.axis(axis) + origin(axis) + cursor(axis)
        targetIndex = targetIndex * storedShape.axis(axis) + cursor(axis)
        axis += 1
      mapping.copyValue(
        source.array,
        sourceIndex.toInt,
        target,
        targetIndex.toInt
      ) match
        case Some(error) => return Left(error)
        case None        => ()
      copied += 1L
      advance(cursor, extent)
    Right(())

  private def advance(cursor: Array[Long], extent: Array[Long]): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < extent(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

/** Synchronous first-party Ravel creation facade. */
object RavelZarr:
  def createArray[D <: DType, R <: AnyRank](
      store: ObjectWriter,
      spec: ArraySpec[D],
      source: RavelArraySource[D, R],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, TypedWriteResult[D]] =
    for
      descriptor <- TypedWriteSupport
        .descriptor(spec, sharding, codecs, chunkKey, capabilities)
        .left
        .map(RavelInteropError.Zarr.apply)
      provider <- source.typedProvider(descriptor)
    yield TypedWriteSupport.result(
      spec,
      descriptor,
      SyncZarrWriter.create(
        store,
        descriptor,
        provider.underlying,
        path,
        limits,
        runtime,
        spec.format
      )
    )

  def createAndOpenArray[D <: DType, R <: AnyRank](
      store: ObjectWriter & ObjectReader,
      spec: ArraySpec[D],
      source: RavelArraySource[D, R],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: SyncCodecRuntime = SyncCodecRuntime.core,
      openLimits: OpenLimits = OpenLimits(),
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using mapping: RavelElement[D]): Either[RavelInteropError, TypedCreateAndOpen[D]] =
    createArray(
      store,
      spec,
      source,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      capabilities
    ).flatMap: result =>
      TypedWriteSupport
        .open(store, result, capabilities, openLimits, runtime, path)
        .left
        .map(RavelInteropError.Zarr.apply)

/** Asynchronous first-party Ravel creation facade. */
object AsyncRavelZarr:
  def createArray[D <: DType, R <: AnyRank](
      store: AsyncObjectWriter,
      spec: ArraySpec[D],
      source: RavelArraySource[D, R],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, TypedWriteResult[D]]] =
    val prepared = for
      descriptor <- TypedWriteSupport
        .descriptor(spec, sharding, codecs, chunkKey, capabilities)
        .left
        .map(RavelInteropError.Zarr.apply)
      provider <- source.typedProvider(descriptor)
    yield descriptor -> provider

    prepared match
      case Left(error)                   => Future.successful(Left(error))
      case Right((descriptor, provider)) =>
        AsyncZarrWriter
          .create(
            store,
            descriptor,
            AsyncChunkProvider.fromSync(provider.underlying),
            path,
            limits,
            runtime,
            spec.format
          )
          .map(outcome => Right(TypedWriteSupport.result(spec, descriptor, outcome)))

  def createAndOpenArray[D <: DType, R <: AnyRank](
      store: AsyncObjectWriter & AsyncObjectReader,
      spec: ArraySpec[D],
      source: RavelArraySource[D, R],
      sharding: Option[ShardingSpec] = None,
      codecs: Vector[ArrayCodecSpec] = Vector.empty,
      chunkKey: Option[ChunkKeySpec] = None,
      path: ZarrPath = ZarrPath.root,
      limits: WriterLimits = WriterLimits(),
      runtime: AsyncCodecRuntime = AsyncCodecRuntime.core,
      openLimits: OpenLimits = OpenLimits(),
      capabilities: ZarrCapabilities = ZarrCapabilities()
  )(using
      mapping: RavelElement[D],
      executionContext: ExecutionContext
  ): Future[Either[RavelInteropError, AsyncTypedCreateAndOpen[D]]] =
    createArray(
      store,
      spec,
      source,
      sharding,
      codecs,
      chunkKey,
      path,
      limits,
      runtime,
      capabilities
    ).flatMap:
      case Left(error)   => Future.successful(Left(error))
      case Right(result) =>
        TypedWriteSupport
          .openAsync(store, result, capabilities, openLimits, runtime, path)
          .map(Right(_))
