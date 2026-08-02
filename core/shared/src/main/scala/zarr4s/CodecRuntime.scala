package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait SyncByteCodecExecutor:
  def name: String

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes]

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes]

trait AsyncByteCodecExecutor:
  def name: String

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]]

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]]

/** Immutable synchronous codec algorithms supplied at an IO boundary. */
final class SyncCodecRuntime private (
    val platform: String,
    private val executors: Map[String, SyncByteCodecExecutor]
):
  def executorNames: Vector[String] = executors.keys.toVector.sorted

  def validate(program: CodecProgram): Either[ZarrError, Unit] =
    CodecRuntimeValidation.validate(
      program,
      platform,
      executors.contains
    )

  private[zarr4s] def decode(
      encoded: OwnedBytes,
      program: CodecProgram,
      dataType: DataTypeCapability,
      decodedShape: Shape,
      limits: DecodeLimits
  ): Either[ZarrError, PrimitiveBlock] =
    CodecRuntimeValidation
      .arrayTrace(program, decodedShape, dataType)
      .flatMap: trace =>
        trace.encodedShape.elementCount.flatMap: elementCount =>
          expectedByteLength(trace.encodedDataType, elementCount).flatMap: expected =>
            if expected.toLong > limits.maxDecodedBytes.toLong then
              Left(
                ZarrError.CodecFailure(
                  CodecError.DecodedLimitExceeded(
                    limits.maxDecodedBytes.toLong,
                    expected.toLong
                  )
                )
              )
            else
              decodeBytesAndArrays(
                encoded,
                program,
                trace.encodedDataType,
                elementCount,
                expected,
                limits,
                trace
              )

  private def decodeBytesAndArrays(
      encoded: OwnedBytes,
      program: CodecProgram,
      encodedDataType: DataTypeCapability,
      elementCount: Long,
      expected: ByteCount,
      limits: DecodeLimits,
      trace: ArrayCodecTrace
  ): Either[ZarrError, PrimitiveBlock] =
    var bytes = encoded
    var byteCodec: Option[BytesCodec] = None
    var index = program.stages.length - 1
    while index >= 0 do
      program.stages(index) match
        case found: BytesCodec => byteCodec = Some(found)
        case stage
            if stage.input == CodecRepresentation.Bytes &&
              stage.output == CodecRepresentation.Bytes =>
          executors.get(stage.name) match
            case None           => return missing(stage.name)
            case Some(executor) =>
              executor.decode(stage, bytes, expected, limits) match
                case Left(error)  => return Left(ZarrError.CodecFailure(error))
                case Right(found) => bytes = found
        case _: ExecutableArrayCodec => ()
        case stage                   =>
          return Left(ZarrError.UnsupportedRead(s"executable array codec ${stage.name}"))
      index -= 1
    byteCodec match
      case None        => Left(ZarrError.InvalidCodecChain("missing bytes codec during decode"))
      case Some(codec) =>
        ScalarBytes
          .decode(bytes, encodedDataType, codec.endianness, elementCount, limits)
          .left
          .map(ZarrError.CodecFailure.apply)
          .flatMap(block => CodecRuntimeValidation.decodeArrays(block, trace))

  private[zarr4s] def encode(
      block: PrimitiveBlock,
      dataType: DataTypeCapability,
      decodedShape: Shape,
      program: CodecProgram,
      maxEncodedBytes: ByteCount
  ): Either[ZarrError, OwnedBytes] =
    val trace = CodecRuntimeValidation.arrayTrace(program, decodedShape, dataType) match
      case Left(error)  => return Left(error)
      case Right(found) => found
    val expectedElements = decodedShape.elementCount match
      case Left(error)  => return Left(error)
      case Right(found) => found
    if block.elementCount.toLong != expectedElements then
      return Left(
        ZarrError.InvalidSelection(
          s"codec input has ${block.elementCount} elements, expected $expectedElements"
        )
      )
    var currentBlock = block
    var currentShape = decodedShape
    var bytes: Option[OwnedBytes] = None
    var index = 0
    while index < program.stages.length do
      program.stages(index) match
        case codec: ExecutableArrayCodec =>
          if bytes.nonEmpty then
            return Left(ZarrError.InvalidCodecChain(s"array codec ${codec.name} follows bytes"))
          codec.encodeArray(currentBlock, currentShape) match
            case Left(error)  => return Left(error)
            case Right(found) =>
              currentBlock = found.block
              currentShape = found.shape
        case codec: BytesCodec =>
          if bytes.nonEmpty then
            return Left(ZarrError.InvalidCodecChain("bytes codec is not the first encoding stage"))
          ScalarBytes.encode(currentBlock, trace.encodedDataType, codec.endianness) match
            case Left(error)  => return Left(ZarrError.CodecFailure(error))
            case Right(found) => bytes = Some(found)
        case stage
            if stage.input == CodecRepresentation.Bytes &&
              stage.output == CodecRepresentation.Bytes =>
          bytes match
            case None =>
              return Left(
                ZarrError.InvalidCodecChain(
                  s"codec ${stage.name} precedes the array-to-bytes stage"
                )
              )
            case Some(found) =>
              executors.get(stage.name) match
                case None           => return missing(stage.name)
                case Some(executor) =>
                  executor.encode(stage, found) match
                    case Left(error)    => return Left(ZarrError.CodecFailure(error))
                    case Right(encoded) => bytes = Some(encoded)
        case stage =>
          return Left(
            ZarrError.UnsupportedRead(
              s"executable array codec ${stage.name}"
            )
          )

      bytes match
        case Some(found) if found.byteCount.toLong > maxEncodedBytes.toLong =>
          return Left(
            ZarrError.ResourceLimit(
              "encoded chunk bytes",
              maxEncodedBytes.toLong,
              found.byteCount.toLong
            )
          )
        case _ => ()
      index += 1
    bytes.toRight(ZarrError.InvalidCodecChain("codec program did not produce bytes"))

  private def missing[A](name: String): Either[ZarrError, A] =
    Left(ZarrError.CodecFailure(CodecError.UnsupportedCapability(name, platform)))

  private def expectedByteLength(
      dataType: DataTypeCapability,
      elementCount: Long
  ): Either[ZarrError, ByteCount] =
    LongArrays
      .checkedMultiply(
        elementCount,
        dataType.byteWidth.toLong,
        "decoded chunk byte length"
      )
      .flatMap(ByteCount.apply)

object SyncCodecRuntime:
  def apply(
      platform: String,
      executors: Seq[SyncByteCodecExecutor] = Seq.empty
  ): Either[ZarrError, SyncCodecRuntime] =
    CodecRuntimeValidation
      .executorIndex(
        platform,
        SyncShuffleExecutor +: SyncCrc32cExecutor +: executors.toVector,
        _.name
      )
      .map(new SyncCodecRuntime(platform, _))

  val core: SyncCodecRuntime =
    new SyncCodecRuntime(
      "synchronous runtime",
      Map("crc32c" -> SyncCrc32cExecutor, "shuffle" -> SyncShuffleExecutor)
    )

  private[zarr4s] def unsafe(
      platform: String,
      executors: Seq[SyncByteCodecExecutor]
  ): SyncCodecRuntime = apply(platform, executors) match
    case Right(found) => found
    case Left(error)  => throw new IllegalArgumentException(error.message)

/** Immutable asynchronous codec algorithms supplied to the Scala.js reader. */
final class AsyncCodecRuntime private (
    val platform: String,
    private val executors: Map[String, AsyncByteCodecExecutor]
):
  def executorNames: Vector[String] = executors.keys.toVector.sorted

  def validate(program: CodecProgram): Either[ZarrError, Unit] =
    CodecRuntimeValidation.validate(
      program,
      platform,
      executors.contains
    )

  private[zarr4s] def decode(
      encoded: OwnedBytes,
      program: CodecProgram,
      dataType: DataTypeCapability,
      decodedShape: Shape,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[ZarrError, PrimitiveBlock]] =
    CodecRuntimeValidation.arrayTrace(program, decodedShape, dataType) match
      case Left(error)  => Future.successful(Left(error))
      case Right(trace) =>
        trace.encodedShape.elementCount match
          case Left(error)         => Future.successful(Left(error))
          case Right(elementCount) =>
            expectedByteLength(trace.encodedDataType, elementCount) match
              case Left(error) => Future.successful(Left(error))
              case Right(expected) if expected.toLong > limits.maxDecodedBytes.toLong =>
                Future.successful(
                  Left(
                    ZarrError.CodecFailure(
                      CodecError.DecodedLimitExceeded(
                        limits.maxDecodedBytes.toLong,
                        expected.toLong
                      )
                    )
                  )
                )
              case Right(expected) =>
                def stage(
                    index: Int,
                    bytes: OwnedBytes
                ): Future[Either[ZarrError, (OwnedBytes, Option[BytesCodec])]] =
                  if index < 0 then Future.successful(Right(bytes -> None))
                  else
                    program.stages(index) match
                      case found: BytesCodec       => Future.successful(Right(bytes -> Some(found)))
                      case _: ExecutableArrayCodec => stage(index - 1, bytes)
                      case codec
                          if codec.input == CodecRepresentation.Bytes &&
                            codec.output == CodecRepresentation.Bytes =>
                        executors.get(codec.name) match
                          case None           => Future.successful(missing(codec.name))
                          case Some(executor) =>
                            executor
                              .decode(codec, bytes, expected, limits)
                              .flatMap:
                                case Left(error) =>
                                  Future.successful(Left(ZarrError.CodecFailure(error)))
                                case Right(found) => stage(index - 1, found)
                      case codec =>
                        Future.successful(
                          Left(
                            ZarrError.UnsupportedRead(
                              s"executable array codec ${codec.name}"
                            )
                          )
                        )

                stage(program.stages.length - 1, encoded).map:
                  case Left(error)      => Left(error)
                  case Right((_, None)) =>
                    Left(ZarrError.InvalidCodecChain("missing bytes codec during decode"))
                  case Right((bytes, Some(codec))) =>
                    ScalarBytes
                      .decode(bytes, trace.encodedDataType, codec.endianness, elementCount, limits)
                      .left
                      .map(ZarrError.CodecFailure.apply)
                      .flatMap(block => CodecRuntimeValidation.decodeArrays(block, trace))

  private[zarr4s] def encode(
      block: PrimitiveBlock,
      dataType: DataTypeCapability,
      decodedShape: Shape,
      program: CodecProgram,
      maxEncodedBytes: ByteCount
  )(using ExecutionContext): Future[Either[ZarrError, OwnedBytes]] =
    val encodedDataType = CodecRuntimeValidation.arrayTrace(program, decodedShape, dataType) match
      case Left(error)  => return Future.successful(Left(error))
      case Right(found) => found.encodedDataType
    decodedShape.elementCount match
      case Left(error) => Future.successful(Left(error))
      case Right(expectedElements) if block.elementCount.toLong != expectedElements =>
        Future.successful(
          Left(
            ZarrError.InvalidSelection(
              s"codec input has ${block.elementCount} elements, expected $expectedElements"
            )
          )
        )
      case Right(_) =>
        def stage(
            index: Int,
            currentBlock: PrimitiveBlock,
            currentShape: Shape,
            bytes: Option[OwnedBytes]
        ): Future[Either[ZarrError, OwnedBytes]] =
          if index >= program.stages.length then
            Future.successful(
              bytes.toRight(
                ZarrError.InvalidCodecChain("codec program did not produce bytes")
              )
            )
          else
            program.stages(index) match
              case codec: ExecutableArrayCodec =>
                if bytes.nonEmpty then
                  Future.successful(
                    Left(
                      ZarrError.InvalidCodecChain(
                        s"array codec ${codec.name} follows bytes"
                      )
                    )
                  )
                else
                  codec.encodeArray(currentBlock, currentShape) match
                    case Left(error)  => Future.successful(Left(error))
                    case Right(found) => stage(index + 1, found.block, found.shape, None)
              case codec: BytesCodec =>
                if bytes.nonEmpty then
                  Future.successful(
                    Left(
                      ZarrError.InvalidCodecChain(
                        "bytes codec is not the first encoding stage"
                      )
                    )
                  )
                else
                  ScalarBytes.encode(currentBlock, encodedDataType, codec.endianness) match
                    case Left(error)  => Future.successful(Left(ZarrError.CodecFailure(error)))
                    case Right(found) =>
                      checked(found, maxEncodedBytes).flatMap:
                        case Left(error)  => Future.successful(Left(error))
                        case Right(value) =>
                          stage(index + 1, currentBlock, currentShape, Some(value))
              case codec
                  if codec.input == CodecRepresentation.Bytes &&
                    codec.output == CodecRepresentation.Bytes =>
                bytes match
                  case None =>
                    Future.successful(
                      Left(
                        ZarrError.InvalidCodecChain(
                          s"codec ${codec.name} precedes the array-to-bytes stage"
                        )
                      )
                    )
                  case Some(found) =>
                    executors.get(codec.name) match
                      case None           => Future.successful(missing(codec.name))
                      case Some(executor) =>
                        executor
                          .encode(codec, found)
                          .flatMap:
                            case Left(error) =>
                              Future.successful(Left(ZarrError.CodecFailure(error)))
                            case Right(encoded) =>
                              checked(encoded, maxEncodedBytes).flatMap:
                                case Left(error)  => Future.successful(Left(error))
                                case Right(value) =>
                                  stage(
                                    index + 1,
                                    currentBlock,
                                    currentShape,
                                    Some(value)
                                  )
              case codec =>
                Future.successful(
                  Left(
                    ZarrError.UnsupportedWrite(
                      s"executable array codec ${codec.name}"
                    )
                  )
                )
        stage(0, block, decodedShape, None)

  private def checked(
      bytes: OwnedBytes,
      limit: ByteCount
  ): Future[Either[ZarrError, OwnedBytes]] =
    if bytes.byteCount.toLong > limit.toLong then
      Future.successful(
        Left(
          ZarrError.ResourceLimit("encoded chunk bytes", limit.toLong, bytes.byteCount.toLong)
        )
      )
    else Future.successful(Right(bytes))

  private def missing[A](name: String): Either[ZarrError, A] =
    Left(ZarrError.CodecFailure(CodecError.UnsupportedCapability(name, platform)))

  private def expectedByteLength(
      dataType: DataTypeCapability,
      elementCount: Long
  ): Either[ZarrError, ByteCount] =
    LongArrays
      .checkedMultiply(
        elementCount,
        dataType.byteWidth.toLong,
        "decoded chunk byte length"
      )
      .flatMap(ByteCount.apply)

object AsyncCodecRuntime:
  def apply(
      platform: String,
      executors: Seq[AsyncByteCodecExecutor] = Seq.empty
  ): Either[ZarrError, AsyncCodecRuntime] =
    CodecRuntimeValidation
      .executorIndex(
        platform,
        AsyncShuffleExecutor +: AsyncCrc32cExecutor +: executors.toVector,
        _.name
      )
      .map(new AsyncCodecRuntime(platform, _))

  val core: AsyncCodecRuntime =
    new AsyncCodecRuntime(
      "asynchronous runtime",
      Map("crc32c" -> AsyncCrc32cExecutor, "shuffle" -> AsyncShuffleExecutor)
    )

  private[zarr4s] def unsafe(
      platform: String,
      executors: Seq[AsyncByteCodecExecutor]
  ): AsyncCodecRuntime = apply(platform, executors) match
    case Right(found) => found
    case Left(error)  => throw new IllegalArgumentException(error.message)

private object CodecRuntimeValidation:
  def validate(
      program: CodecProgram,
      platform: String,
      contains: String => Boolean
  ): Either[ZarrError, Unit] =
    var index = 0
    while index < program.stages.length do
      program.stages(index) match
        case _: BytesCodec           => ()
        case _: ExecutableArrayCodec => ()
        case stage
            if stage.input == CodecRepresentation.Bytes &&
              stage.output == CodecRepresentation.Bytes =>
          if !contains(stage.name) then
            return Left(
              ZarrError.CodecFailure(
                CodecError.UnsupportedCapability(stage.name, platform)
              )
            )
        case stage =>
          return Left(
            ZarrError.UnsupportedRead(
              s"executable array codec ${stage.name}"
            )
          )
      index += 1
    Right(())

  def arrayTrace(
      program: CodecProgram,
      decodedShape: Shape,
      decodedDataType: DataTypeCapability
  ): Either[ZarrError, ArrayCodecTrace] =
    val stages = Vector.newBuilder[ArrayCodecStage]
    var shape = decodedShape
    var dataType = decodedDataType
    var index = 0
    while index < program.stages.length do
      program.stages(index) match
        case codec: ExecutableArrayCodec =>
          stages += ArrayCodecStage(codec, shape)
          codec.encodedShape(shape) match
            case Left(error)  => return Left(error)
            case Right(found) => shape = found
          codec.encodedDataType(dataType) match
            case Left(error)  => return Left(error)
            case Right(found) => dataType = found
        case _: BytesCodec                                     => ()
        case stage if stage.input == CodecRepresentation.Bytes => ()
        case stage                                             =>
          return Left(ZarrError.UnsupportedRead(s"executable array codec ${stage.name}"))
      index += 1
    Right(ArrayCodecTrace(stages.result(), shape, dataType))

  def decodeArrays(
      encoded: PrimitiveBlock,
      trace: ArrayCodecTrace
  ): Either[ZarrError, PrimitiveBlock] =
    var block = encoded
    var index = trace.stages.length - 1
    while index >= 0 do
      val stage = trace.stages(index)
      stage.codec.decodeArray(block, stage.decodedShape) match
        case Left(error)  => return Left(error)
        case Right(found) => block = found
      index -= 1
    Right(block)

  def executorIndex[A](
      platform: String,
      executors: Vector[A],
      name: A => String
  ): Either[ZarrError, Map[String, A]] =
    if platform.trim.isEmpty then Left(ZarrError.InvalidCodecRuntime("platform must be non-empty"))
    else
      val result = Map.newBuilder[String, A]
      val seen = scala.collection.mutable.HashSet.empty[String]
      var index = 0
      while index < executors.length do
        val found = name(executors(index))
        if found.trim.isEmpty then
          return Left(ZarrError.InvalidCodecRuntime("executor name must be non-empty"))
        if seen.contains(found) then
          return Left(ZarrError.InvalidCodecRuntime(s"duplicate executor '$found' on $platform"))
        seen += found
        result += found -> executors(index)
        index += 1
      Right(result.result())

private final case class ArrayCodecStage(
    codec: ExecutableArrayCodec,
    decodedShape: Shape
)

private final case class ArrayCodecTrace(
    stages: Vector[ArrayCodecStage],
    encodedShape: Shape,
    encodedDataType: DataTypeCapability
)

private object SyncCrc32cExecutor extends SyncByteCodecExecutor:
  val name = "crc32c"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = Crc32c.verifyAndStrip(encoded)

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = Right(Crc32c.append(decoded))

private object AsyncCrc32cExecutor extends AsyncByteCodecExecutor:
  val name = "crc32c"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(Crc32c.verifyAndStrip(encoded))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
    Future.successful(Right(Crc32c.append(decoded)))

private object SyncShuffleExecutor extends SyncByteCodecExecutor:
  val name = "shuffle"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] = codec match
    case found: ShuffleCodec => Shuffle.decode(encoded, expectedDecoded, limits, found.elementSize)
    case found               =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes] = codec match
    case found: ShuffleCodec => Shuffle.encode(decoded, found.elementSize)
    case found               =>
      Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))

private object AsyncShuffleExecutor extends AsyncByteCodecExecutor:
  val name = "shuffle"

  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case found: ShuffleCodec =>
      Future.successful(Shuffle.decode(encoded, expectedDecoded, limits, found.elementSize))
    case found =>
      Future.successful(
        Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))
      )

  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] = codec match
    case found: ShuffleCodec => Future.successful(Shuffle.encode(decoded, found.elementSize))
    case found               =>
      Future.successful(
        Left(CodecError.CorruptData(name, s"executor received compiled codec ${found.name}"))
      )
