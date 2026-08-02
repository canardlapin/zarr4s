package zarr4s

/** A validated codec chain.
  *
  * The constructor is private so a descriptor cannot contain an impossible representation
  * transition. Programs beginning with array values contain exactly one array-to-bytes transition;
  * programs beginning with bytes are lawful byte identities or bytes-to-bytes chains.
  */
final class CodecProgram private (
    val initial: CodecRepresentation,
    val stages: Vector[CompiledCodec]
):
  val output: CodecRepresentation =
    stages.lastOption.fold(initial)(_.output)

  val executorRequirements: Vector[String] =
    stages.iterator
      .filter(stage => stage.input == CodecRepresentation.Bytes)
      .map(_.name)
      .toVector
      .distinct

  def isEmpty: Boolean = stages.isEmpty
  def nonEmpty: Boolean = stages.nonEmpty

  /** Shape immediately before the array-to-bytes transition. */
  def encodedArrayShape(decodedShape: Shape): Either[ZarrError, Shape] =
    if initial == CodecRepresentation.Bytes then Right(decodedShape)
    else
      var shape = decodedShape
      var index = 0
      while index < stages.length do
        stages(index) match
          case codec: ExecutableArrayCodec =>
            codec.encodedShape(shape) match
              case Left(error)  => return Left(error)
              case Right(found) => shape = found
          case _: BytesCodec                                     => return Right(shape)
          case stage if stage.input == CodecRepresentation.Bytes => return Right(shape)
          case stage                                             =>
            return Left(ZarrError.UnsupportedRead(s"executable array codec ${stage.name}"))
        index += 1
      Right(shape)

  /** Data type immediately before the array-to-bytes transition. */
  def encodedArrayDataType(
      decodedDataType: DataTypeCapability
  ): Either[ZarrError, DataTypeCapability] =
    if initial == CodecRepresentation.Bytes then Right(decodedDataType)
    else
      var dataType = decodedDataType
      var index = 0
      while index < stages.length do
        stages(index) match
          case codec: ExecutableArrayCodec =>
            codec.encodedDataType(dataType) match
              case Left(error)  => return Left(error)
              case Right(found) => dataType = found
          case _: BytesCodec                                     => return Right(dataType)
          case stage if stage.input == CodecRepresentation.Bytes => return Right(dataType)
          case stage                                             =>
            return Left(ZarrError.UnsupportedRead(s"executable array codec ${stage.name}"))
        index += 1
      Right(dataType)

  override def equals(other: Any): Boolean = other match
    case that: CodecProgram => initial == that.initial && stages == that.stages
    case _                  => false

  override def hashCode(): Int = 31 * initial.hashCode + stages.hashCode

  override def toString: String =
    s"CodecProgram($initial,${stages.mkString("[", ",", "]")})"

object CodecProgram:
  def compile(
      initial: CodecRepresentation,
      stages: Vector[CompiledCodec]
  ): Either[ZarrError, CodecProgram] =
    var representation = initial
    var transitionsToBytes = 0
    var index = 0
    while index < stages.length do
      val stage = stages(index)
      if stage.input != representation then
        return Left(
          ZarrError.InvalidCodecChain(
            s"codec ${stage.name} at index $index requires ${stage.input}, found $representation"
          )
        )
      if stage.input == CodecRepresentation.ArrayValues &&
        stage.output == CodecRepresentation.Bytes
      then transitionsToBytes += 1
      representation = stage.output
      index += 1

    if representation != CodecRepresentation.Bytes then
      Left(ZarrError.InvalidCodecChain("codec chain must transform its input into bytes"))
    else if initial == CodecRepresentation.ArrayValues && transitionsToBytes != 1 then
      Left(
        ZarrError.InvalidCodecChain(
          s"array codec chain must contain exactly one array-to-bytes stage, found $transitionsToBytes"
        )
      )
    else if initial == CodecRepresentation.Bytes && transitionsToBytes != 0 then
      Left(ZarrError.InvalidCodecChain("bytes codec chain cannot contain an array-to-bytes stage"))
    else Right(new CodecProgram(initial, stages))

  val bytesIdentity: CodecProgram =
    new CodecProgram(CodecRepresentation.Bytes, Vector.empty)

/** The executable Zarr v3 shard-index profile supported by the kernel.
  *
  * Keeping this as a value in `PhysicalLayout` makes the index pipeline inspectable and leaves an
  * honest extension point without pretending the current uint64 index implementation supports
  * arbitrary codecs.
  */
final class ShardIndexProgram private (val codecs: CodecProgram):
  override def equals(other: Any): Boolean = other match
    case that: ShardIndexProgram => codecs == that.codecs
    case _                       => false

  override def hashCode(): Int = codecs.hashCode

  override def toString: String = s"ShardIndexProgram($codecs)"

object ShardIndexProgram:
  private[zarr4s] def compile(stages: Vector[CompiledCodec]): Either[ZarrError, ShardIndexProgram] =
    stages match
      case Vector(BytesCodec(Some(Endianness.Little)), Crc32cCodec) =>
        CodecProgram.compile(CodecRepresentation.ArrayValues, stages).map(new ShardIndexProgram(_))
      case _ =>
        Left(
          ZarrError.UnsupportedExtension(
            "shard index codec pipeline",
            stages.map(_.name).mkString("[", ",", "]")
          )
        )
