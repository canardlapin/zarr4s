package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** A provider whose dtype witness is part of the public type. */
final class TypedChunkProvider[D <: DType] private[zarr4s] (
    val dtype: D,
    private[zarr4s] val underlying: ChunkProvider
)

object TypedChunkProvider:
  /** Adapt an advanced provider after attaching the dtype it promises to emit. */
  def from[D <: DType](dtype: D, provider: ChunkProvider): TypedChunkProvider[D] =
    new TypedChunkProvider(dtype, provider)

/** User-controlled metadata for creating a group.
  *
  * Parsed unknown fields remain part of [[GroupMetadata]] and are intentionally absent here: a new
  * group contains only the attributes and format selected by its creator.
  */
final case class GroupSpec(
    attributes: JsonObject = JsonObject.empty,
    format: ZarrFormat = ZarrFormat.V3
):
  def withAttributes(value: JsonObject): GroupSpec = copy(attributes = value)
  def asFormat(value: ZarrFormat): GroupSpec = copy(format = value)

/** High-level group creation result retaining complete or incomplete publication progress. */
final case class GroupWriteResult(spec: GroupSpec, outcome: WriteOutcome):
  def receipt: Option[WriteReceipt] = outcome match
    case WriteOutcome.Complete(found)  => Some(found)
    case WriteOutcome.Incomplete(_, _) => None

/** The complete high-level creation result, including incomplete writer progress. */
final case class TypedWriteResult[D <: DType](
    spec: ArraySpec[D],
    descriptor: ArrayDescriptor,
    outcome: WriteOutcome
):
  def receipt: Option[WriteReceipt] = outcome match
    case WriteOutcome.Complete(found)  => Some(found)
    case WriteOutcome.Incomplete(_, _) => None

/** A creation result paired with a checked handle when the store can also be read. */
final case class TypedCreateAndOpen[D <: DType](
    write: TypedWriteResult[D],
    opened: Either[ZarrError, TypedOpenedArray[D]]
):
  def descriptor: ArrayDescriptor = write.descriptor
  def outcome: WriteOutcome = write.outcome

/** Asynchronous create-and-open result with the async typed handle. */
final case class AsyncTypedCreateAndOpen[D <: DType](
    write: TypedWriteResult[D],
    opened: Either[ZarrError, AsyncTypedOpenedArray[D]]
):
  def descriptor: ArrayDescriptor = write.descriptor
  def outcome: WriteOutcome = write.outcome

private[zarr4s] object TypedWriteSupport:
  def descriptor[D <: DType](
      spec: ArraySpec[D],
      sharding: Option[ShardingSpec],
      codecs: Vector[ArrayCodecSpec],
      chunkKey: Option[ChunkKeySpec],
      capabilities: ZarrCapabilities
  ): Either[ZarrError, ArrayDescriptor] = sharding match
    case None        => ArrayDescriptor.direct(spec, codecs, chunkKey, capabilities)
    case Some(found) => ArrayDescriptor.sharded(spec, found, codecs, chunkKey, capabilities)

  def denseProvider[D <: DType](
      descriptor: ArrayDescriptor,
      spec: ArraySpec[D],
      data: DenseArray[D]
  ): Either[ZarrError, ChunkProvider] =
    if spec.dtype.dataType.name != data.dtype.dataType.name then
      Left(ZarrError.DTypeMismatch(spec.dtype.name, data.dtype.name, "array creation"))
    else if spec.shape != data.shape then
      Left(
        ZarrError.InvalidShape(
          s"dense value shape ${data.shape} does not match specification shape ${spec.shape}"
        )
      )
    else ChunkProvider.fromDense(descriptor, data)

  def typedProvider[D <: DType](
      spec: ArraySpec[D],
      provider: TypedChunkProvider[D]
  ): Either[ZarrError, ChunkProvider] =
    if spec.dtype.dataType.name != provider.dtype.dataType.name then
      Left(ZarrError.DTypeMismatch(spec.dtype.name, provider.dtype.name, "array creation"))
    else Right(provider.underlying)

  def result[D <: DType](
      spec: ArraySpec[D],
      descriptor: ArrayDescriptor,
      outcome: WriteOutcome
  ): TypedWriteResult[D] = TypedWriteResult(spec, descriptor, outcome)

  def open[D <: DType](
      store: ObjectWriter & ObjectReader,
      result: TypedWriteResult[D],
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: SyncCodecRuntime,
      path: ZarrPath
  ): Either[ZarrError, TypedCreateAndOpen[D]] =
    val opened = result.outcome match
      case WriteOutcome.Incomplete(_, error) => Left(error)
      case WriteOutcome.Complete(_)          =>
        SyncZarr.openTypedArray(
          store,
          result.spec.dtype,
          path,
          capabilities,
          limits,
          runtime
        )
    Right(TypedCreateAndOpen(result, opened))

  def openAsync[D <: DType](
      store: AsyncObjectWriter & AsyncObjectReader,
      result: TypedWriteResult[D],
      capabilities: ZarrCapabilities,
      limits: OpenLimits,
      runtime: AsyncCodecRuntime,
      path: ZarrPath
  )(using ExecutionContext): Future[AsyncTypedCreateAndOpen[D]] =
    result.outcome match
      case WriteOutcome.Incomplete(_, error) =>
        Future.successful(AsyncTypedCreateAndOpen(result, Left(error)))
      case WriteOutcome.Complete(_) =>
        AsyncZarr
          .openTypedArray(
            store,
            result.spec.dtype,
            path,
            capabilities,
            limits,
            runtime
          )
          .map(opened => AsyncTypedCreateAndOpen(result, opened))
