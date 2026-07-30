package zarr4s

final case class ExtensionMetadata(
    name: String,
    configuration: JsonObject,
    mustUnderstand: Boolean,
    extra: JsonObject
)

enum ZarrNodeMetadata:
  case Group(metadata: GroupMetadata)
  case Array(metadata: ArrayMetadata)

final case class GroupMetadata(attributes: JsonObject, unknown: JsonObject)

final case class ArrayMetadata(
    shape: Vector[Long],
    dataType: ExtensionMetadata,
    chunkGrid: ExtensionMetadata,
    chunkKeyEncoding: ExtensionMetadata,
    fillValue: JsonValue,
    codecs: Vector[ExtensionMetadata],
    attributes: JsonObject,
    dimensionNames: Option[Vector[Option[String]]],
    storageTransformers: Vector[ExtensionMetadata],
    unknown: JsonObject
)

enum StoredScalar:
  case Boolean(value: scala.Boolean)
  case Integral(value: Long)
  case UnsignedIntegral(value: BigInt)
  case Floating(value: Double)
  case FloatingBits(hex: String)

trait DataTypeCapability:
  def name: String
  def scalarKind: ScalarKind
  final def byteWidth: Int = scalarKind.byteWidth
  def parseFill(value: JsonValue): Either[String, StoredScalar]

object BuiltInDataTypes:
  val bool: DataTypeCapability = BooleanDataType
  val int8: DataTypeCapability =
    SignedIntegerDataType("int8", ScalarKind.Signed8, Byte.MinValue, Byte.MaxValue)
  val int16: DataTypeCapability =
    SignedIntegerDataType("int16", ScalarKind.Signed16, Short.MinValue, Short.MaxValue)
  val int32: DataTypeCapability =
    SignedIntegerDataType("int32", ScalarKind.Signed32, Int.MinValue, Int.MaxValue)
  val int64: DataTypeCapability =
    SignedIntegerDataType("int64", ScalarKind.Signed64, Long.MinValue, Long.MaxValue)
  val uint8: DataTypeCapability = UnsignedIntegerDataType("uint8", ScalarKind.Unsigned8, 8)
  val uint16: DataTypeCapability = UnsignedIntegerDataType("uint16", ScalarKind.Unsigned16, 16)
  val uint32: DataTypeCapability = UnsignedIntegerDataType("uint32", ScalarKind.Unsigned32, 32)
  val uint64: DataTypeCapability = UnsignedIntegerDataType("uint64", ScalarKind.Unsigned64, 64)
  val float32: DataTypeCapability = FloatingDataType("float32", ScalarKind.Float32, 8)
  val float64: DataTypeCapability = FloatingDataType("float64", ScalarKind.Float64, 16)

  val all: Vector[DataTypeCapability] = Vector(
    bool,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float32,
    float64
  )

  private case object BooleanDataType extends DataTypeCapability:
    val name = "bool"
    val scalarKind = ScalarKind.Bool

    def parseFill(value: JsonValue): Either[String, StoredScalar] = value match
      case JsonValue.Bool(found) => Right(StoredScalar.Boolean(found))
      case _                     => Left("bool fill value must be a JSON boolean")

  private final case class SignedIntegerDataType(
      name: String,
      scalarKind: ScalarKind,
      minimum: Long,
      maximum: Long
  ) extends DataTypeCapability:
    def parseFill(value: JsonValue): Either[String, StoredScalar] = value match
      case JsonValue.Num(number) =>
        number.toLongExact.flatMap: found =>
          if found < minimum || found > maximum then
            Left(s"fill value $found is outside the $name range [$minimum, $maximum]")
          else Right(StoredScalar.Integral(found))
      case _ => Left(s"$name fill value must be a JSON integer")

  private final case class UnsignedIntegerDataType(
      name: String,
      scalarKind: ScalarKind,
      bits: Int
  ) extends DataTypeCapability:
    private val maximum = (BigInt(1) << bits) - 1

    def parseFill(value: JsonValue): Either[String, StoredScalar] = value match
      case JsonValue.Num(number) =>
        number.toBigIntExact.flatMap: found =>
          if found < 0 || found > maximum then
            Left(s"fill value $found is outside the $name range [0, $maximum]")
          else Right(StoredScalar.UnsignedIntegral(found))
      case _ => Left(s"$name fill value must be a JSON integer")

  private final case class FloatingDataType(
      name: String,
      scalarKind: ScalarKind,
      bitHexDigits: Int
  ) extends DataTypeCapability:
    def parseFill(value: JsonValue): Either[String, StoredScalar] = value match
      case JsonValue.Num(number)      => Right(StoredScalar.Floating(number.toDouble))
      case JsonValue.Str("NaN")       => Right(StoredScalar.Floating(Double.NaN))
      case JsonValue.Str("Infinity")  => Right(StoredScalar.Floating(Double.PositiveInfinity))
      case JsonValue.Str("-Infinity") => Right(StoredScalar.Floating(Double.NegativeInfinity))
      case JsonValue.Str(hex) if validBits(hex) => Right(StoredScalar.FloatingBits(hex.toLowerCase))
      case JsonValue.Str(found)                 =>
        Left(s"unsupported $name floating fill value '$found'")
      case _ => Left(s"$name fill value must be a JSON number or floating-point sentinel")

    private def validBits(value: String): Boolean =
      value.length == bitHexDigits + 2 &&
        value.startsWith("0x") &&
        value.drop(2).forall(character => Character.digit(character, 16) >= 0)

enum CodecRepresentation:
  case ArrayValues
  case Bytes

trait CompiledCodec:
  def name: String
  def input: CodecRepresentation
  def output: CodecRepresentation
  def configuration: JsonObject

enum Endianness:
  case Little
  case Big

final case class BytesCodec(endianness: Option[Endianness]) extends CompiledCodec:
  val name = "bytes"
  val input = CodecRepresentation.ArrayValues
  val output = CodecRepresentation.Bytes
  val configuration: JsonObject = endianness match
    case None        => JsonObject.empty
    case Some(found) =>
      JsonObject.unsafe(
        Vector(
          "endian" -> JsonValue.Str(found match
            case Endianness.Little => "little"
            case Endianness.Big    => "big")
        )
      )

final case class GzipCodec(level: Int) extends CompiledCodec:
  val name = "gzip"
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes
  val configuration: JsonObject = JsonObject.unsafe(
    Vector(
      "level" -> JsonValue.Num(JsonNumber.unsafe(level.toString))
    )
  )

case object Crc32cCodec extends CompiledCodec:
  val name = "crc32c"
  val input = CodecRepresentation.Bytes
  val output = CodecRepresentation.Bytes
  val configuration: JsonObject = JsonObject.empty

trait CodecCapability:
  def name: String
  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec]

object BuiltInCodecs:
  val transpose: CodecCapability = new CodecCapability:
    val name = "transpose"

    def compile(
        extension: ExtensionMetadata,
        dataType: DataTypeCapability
    ): Either[String, CompiledCodec] =
      requiredOrder(extension.configuration, "order").flatMap(TransposeCodec.from)

  val bytes: CodecCapability = new CodecCapability:
    val name = "bytes"

    def compile(
        extension: ExtensionMetadata,
        dataType: DataTypeCapability
    ): Either[String, CompiledCodec] =
      optionalString(extension.configuration, "endian").flatMap: configured =>
        val endianness = configured match
          case None           => Right(None)
          case Some("little") => Right(Some(Endianness.Little))
          case Some("big")    => Right(Some(Endianness.Big))
          case Some(found)    => Left(s"bytes endian must be 'little' or 'big', found '$found'")
        endianness.flatMap: found =>
          if dataType.byteWidth > 1 && found.isEmpty then
            Left(s"bytes endian is required for multibyte data type ${dataType.name}")
          else Right(BytesCodec(found))

  val gzip: CodecCapability = new CodecCapability:
    val name = "gzip"

    def compile(
        extension: ExtensionMetadata,
        dataType: DataTypeCapability
    ): Either[String, CompiledCodec] =
      requiredLong(extension.configuration, "level").flatMap: level =>
        if level < 0L || level > 9L then Left(s"gzip level must be in [0, 9], found $level")
        else Right(GzipCodec(level.toInt))

  val crc32c: CodecCapability = new CodecCapability:
    val name = "crc32c"

    def compile(
        extension: ExtensionMetadata,
        dataType: DataTypeCapability
    ): Either[String, CompiledCodec] = Right(Crc32cCodec)

  val all: Vector[CodecCapability] = Vector(transpose, bytes, gzip, crc32c)

  private def requiredOrder(objectValue: JsonObject, field: String): Either[String, Vector[Int]] =
    objectValue.get(field) match
      case Some(JsonValue.Arr(values)) =>
        val result = Vector.newBuilder[Int]
        var index = 0
        while index < values.length do
          values(index) match
            case JsonValue.Num(number) =>
              number.toLongExact match
                case Right(found) if found >= Int.MinValue.toLong && found <= Int.MaxValue.toLong =>
                  result += found.toInt
                case Right(found) =>
                  return Left(s"$field[$index] is outside the 32-bit integer range: $found")
                case Left(detail) => return Left(s"$field[$index] $detail")
            case _ => return Left(s"$field[$index] must be an integer")
          index += 1
        Right(result.result())
      case Some(_) => Left(s"$field must be an array")
      case None    => Left(s"missing required field '$field'")

  private def optionalString(
      objectValue: JsonObject,
      field: String
  ): Either[String, Option[String]] =
    objectValue.get(field) match
      case None                       => Right(None)
      case Some(JsonValue.Str(value)) => Right(Some(value))
      case Some(_)                    => Left(s"$field must be a string")

  private def requiredLong(objectValue: JsonObject, field: String): Either[String, Long] =
    objectValue.get(field) match
      case Some(JsonValue.Num(number)) => number.toLongExact
      case Some(_)                     => Left(s"$field must be an integer")
      case None                        => Left(s"missing required field '$field'")

final case class ZarrCapabilities(
    dataTypes: Vector[DataTypeCapability] = BuiltInDataTypes.all,
    codecs: Vector[CodecCapability] = BuiltInCodecs.all
):
  private val dataTypeIndex = dataTypes.map(capability => capability.name -> capability).toMap
  private val codecIndex = codecs.map(capability => capability.name -> capability).toMap

  def dataType(name: String): Option[DataTypeCapability] = dataTypeIndex.get(name)
  def codec(name: String): Option[CodecCapability] = codecIndex.get(name)

enum PhysicalLayout:
  case Direct(codecs: CodecProgram)
  case Sharded(
      grid: ShardedGrid,
      innerCodecs: CodecProgram,
      indexCodecs: ShardIndexProgram,
      indexLocation: IndexLocation,
      outerCodecs: CodecProgram
  )

final case class ArrayDescriptor(
    shape: Shape,
    dataType: DataTypeCapability,
    fillValue: StoredScalar,
    grid: RegularGrid,
    chunkKeyEncoding: ChunkKeyEncoding,
    layout: PhysicalLayout,
    dimensionNames: Option[Vector[Option[String]]],
    attributes: JsonObject,
    unknown: JsonObject
)

object ZarrMetadata:
  private val groupFields = Set("zarr_format", "node_type", "attributes")
  private val arrayFields = Set(
    "zarr_format",
    "node_type",
    "shape",
    "data_type",
    "chunk_grid",
    "chunk_key_encoding",
    "fill_value",
    "codecs",
    "attributes",
    "dimension_names",
    "storage_transformers"
  )

  def parse(input: String, limits: JsonLimits = JsonLimits()): Either[ZarrError, ZarrNodeMetadata] =
    JsonParser
      .parse(input, limits)
      .left
      .map(ZarrError.InvalidJson.apply)
      .flatMap:
        case JsonValue.Obj(root) => parseRoot(root)
        case _ => Left(ZarrError.InvalidMetadata("$", "root metadata must be an object"))

  private def parseRoot(root: JsonObject): Either[ZarrError, ZarrNodeMetadata] =
    for
      version <- requiredLong(root, "zarr_format", "$.zarr_format")
      _ <- if version == 3L then Right(()) else Left(ZarrError.UnsupportedVersion(version))
      nodeType <- requiredString(root, "node_type", "$.node_type")
      result <- nodeType match
        case "group" => parseGroup(root).map(ZarrNodeMetadata.Group.apply)
        case "array" => parseArray(root).map(ZarrNodeMetadata.Array.apply)
        case found   => Left(ZarrError.UnsupportedNodeType(found))
    yield result

  private def parseGroup(root: JsonObject): Either[ZarrError, GroupMetadata] =
    optionalObject(root, "attributes", "$.attributes").map: attributes =>
      GroupMetadata(attributes.getOrElse(JsonObject.empty), root.removed(groupFields))

  private def parseArray(root: JsonObject): Either[ZarrError, ArrayMetadata] =
    for
      shape <- requiredLongArray(root, "shape", "$.shape", nonNegative = true)
      dataTypeValue <- required(root, "data_type", "$.data_type")
      dataType <- parseExtension(dataTypeValue, "$.data_type")
      chunkGridValue <- required(root, "chunk_grid", "$.chunk_grid")
      chunkGrid <- parseExtension(chunkGridValue, "$.chunk_grid")
      chunkKeyValue <- required(root, "chunk_key_encoding", "$.chunk_key_encoding")
      chunkKey <- parseExtension(chunkKeyValue, "$.chunk_key_encoding")
      fill <- required(root, "fill_value", "$.fill_value")
      codecs <- requiredExtensions(root, "codecs", "$.codecs")
      attributes <- optionalObject(root, "attributes", "$.attributes")
      names <- optionalDimensionNames(root, shape.length)
      transformers <- optionalExtensions(root, "storage_transformers", "$.storage_transformers")
    yield ArrayMetadata(
      shape,
      dataType,
      chunkGrid,
      chunkKey,
      fill,
      codecs,
      attributes.getOrElse(JsonObject.empty),
      names,
      transformers,
      root.removed(arrayFields)
    )

  private[zarr4s] def parseExtension(
      value: JsonValue,
      path: String
  ): Either[ZarrError, ExtensionMetadata] = value match
    case JsonValue.Str(name) =>
      Right(ExtensionMetadata(name, JsonObject.empty, true, JsonObject.empty))
    case JsonValue.Obj(extension) =>
      for
        name <- requiredString(extension, "name", s"$path.name")
        configuration <- optionalObject(extension, "configuration", s"$path.configuration")
        mustUnderstand <- optionalBoolean(extension, "must_understand", s"$path.must_understand")
      yield ExtensionMetadata(
        name,
        configuration.getOrElse(JsonObject.empty),
        mustUnderstand.getOrElse(true),
        extension.removed(Set("name", "configuration", "must_understand"))
      )
    case _ => Left(ZarrError.InvalidMetadata(path, "extension must be a name string or object"))

  private def requiredExtensions(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    required(root, name, path).flatMap:
      case JsonValue.Arr(values) => parseExtensions(values, path)
      case _                     => Left(ZarrError.InvalidMetadata(path, "must be an array"))

  private def optionalExtensions(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Vector[ExtensionMetadata]] = root.get(name) match
    case None                        => Right(Vector.empty)
    case Some(JsonValue.Arr(values)) => parseExtensions(values, path)
    case Some(_)                     => Left(ZarrError.InvalidMetadata(path, "must be an array"))

  private[zarr4s] def parseExtensions(
      values: Vector[JsonValue],
      path: String
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    val result = Vector.newBuilder[ExtensionMetadata]
    var index = 0
    while index < values.length do
      parseExtension(values(index), s"$path[$index]") match
        case Left(error)      => return Left(error)
        case Right(extension) => result += extension
      index += 1
    Right(result.result())

  private def optionalDimensionNames(
      root: JsonObject,
      rank: Int
  ): Either[ZarrError, Option[Vector[Option[String]]]] = root.get("dimension_names") match
    case None                        => Right(None)
    case Some(JsonValue.Arr(values)) =>
      if values.length != rank then
        Left(
          ZarrError.InvalidMetadata(
            "$.dimension_names",
            s"length ${values.length} does not match rank $rank"
          )
        )
      else
        val result = Vector.newBuilder[Option[String]]
        var index = 0
        while index < values.length do
          values(index) match
            case JsonValue.Null      => result += None
            case JsonValue.Str(name) => result += Some(name)
            case _                   =>
              return Left(
                ZarrError.InvalidMetadata(
                  s"$$.dimension_names[$index]",
                  "must be a string or null"
                )
              )
          index += 1
        Right(Some(result.result()))
    case Some(_) => Left(ZarrError.InvalidMetadata("$.dimension_names", "must be an array"))

  private def requiredLongArray(
      root: JsonObject,
      name: String,
      path: String,
      nonNegative: Boolean
  ): Either[ZarrError, Vector[Long]] = required(root, name, path) match
    case Left(error)                  => Left(error)
    case Right(JsonValue.Arr(values)) =>
      val result = Vector.newBuilder[Long]
      var index = 0
      while index < values.length do
        values(index) match
          case JsonValue.Num(number) =>
            number.toLongExact match
              case Left(detail) => return Left(ZarrError.InvalidMetadata(s"$path[$index]", detail))
              case Right(found) if nonNegative && found < 0L =>
                return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be non-negative"))
              case Right(found) => result += found
          case _ => return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be an integer"))
        index += 1
      Right(result.result())
    case Right(_) => Left(ZarrError.InvalidMetadata(path, "must be an array"))

  private def required(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, JsonValue] =
    root.get(name).toRight(ZarrError.InvalidMetadata(path, "missing required field"))

  private def requiredString(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, String] = required(root, name, path).flatMap:
    case JsonValue.Str(value) => Right(value)
    case _                    => Left(ZarrError.InvalidMetadata(path, "must be a string"))

  private def requiredLong(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Long] = required(root, name, path).flatMap:
    case JsonValue.Num(number) =>
      number.toLongExact.left.map(detail => ZarrError.InvalidMetadata(path, detail))
    case _ => Left(ZarrError.InvalidMetadata(path, "must be an integer"))

  private def optionalObject(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Option[JsonObject]] = root.get(name) match
    case None                       => Right(None)
    case Some(JsonValue.Obj(value)) => Right(Some(value))
    case Some(_)                    => Left(ZarrError.InvalidMetadata(path, "must be an object"))

  private def optionalBoolean(
      root: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Option[Boolean]] = root.get(name) match
    case None                        => Right(None)
    case Some(JsonValue.Bool(value)) => Right(Some(value))
    case Some(_)                     => Left(ZarrError.InvalidMetadata(path, "must be a boolean"))

object ArrayDescriptor:
  def compile(
      metadata: ArrayMetadata,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, ArrayDescriptor] =
    for
      shape <- Shape.from(metadata.shape)
      dataType <- compileDataType(metadata.dataType, capabilities)
      fill <- dataType
        .parseFill(metadata.fillValue)
        .left
        .map: detail =>
          ZarrError.InvalidMetadata("$.fill_value", detail)
      grid <- compileGrid(shape, metadata.chunkGrid)
      keyEncoding <- compileChunkKey(metadata.chunkKeyEncoding)
      _ <- compileStorageTransformers(metadata.storageTransformers)
      layout <- compileLayout(metadata.codecs, dataType, grid, capabilities)
    yield ArrayDescriptor(
      shape,
      dataType,
      fill,
      grid,
      keyEncoding,
      layout,
      metadata.dimensionNames,
      metadata.attributes,
      metadata.unknown
    )

  private def compileDataType(
      extension: ExtensionMetadata,
      capabilities: ZarrCapabilities
  ): Either[ZarrError, DataTypeCapability] =
    if !extension.mustUnderstand then
      Left(ZarrError.InvalidMetadata("$.data_type.must_understand", "false is not permitted"))
    else
      capabilities.dataType(extension.name).toRight(ZarrError.UnsupportedDataType(extension.name))

  private def compileGrid(
      shape: Shape,
      extension: ExtensionMetadata
  ): Either[ZarrError, RegularGrid] =
    if !extension.mustUnderstand then
      Left(ZarrError.InvalidMetadata("$.chunk_grid.must_understand", "false is not permitted"))
    else if extension.name != "regular" then
      Left(ZarrError.UnsupportedExtension("chunk grid", extension.name))
    else
      extension.configuration.get("chunk_shape") match
        case Some(JsonValue.Arr(values)) =>
          val dimensions = Vector.newBuilder[Long]
          var index = 0
          while index < values.length do
            values(index) match
              case JsonValue.Num(number) =>
                number.toLongExact match
                  case Right(found) => dimensions += found
                  case Left(detail) =>
                    return Left(
                      ZarrError.InvalidMetadata(
                        s"$$.chunk_grid.configuration.chunk_shape[$index]",
                        detail
                      )
                    )
              case _ =>
                return Left(
                  ZarrError.InvalidMetadata(
                    s"$$.chunk_grid.configuration.chunk_shape[$index]",
                    "must be an integer"
                  )
                )
            index += 1
          Shape.from(dimensions.result()).flatMap(chunkShape => RegularGrid(shape, chunkShape))
        case Some(_) =>
          Left(
            ZarrError.InvalidMetadata(
              "$.chunk_grid.configuration.chunk_shape",
              "must be an array"
            )
          )
        case None =>
          Left(
            ZarrError.InvalidMetadata(
              "$.chunk_grid.configuration.chunk_shape",
              "missing required field"
            )
          )

  private def compileChunkKey(
      extension: ExtensionMetadata
  ): Either[ZarrError, ChunkKeyEncoding] =
    if !extension.mustUnderstand then
      Left(
        ZarrError.InvalidMetadata("$.chunk_key_encoding.must_understand", "false is not permitted")
      )
    else
      extension.name match
        case "default" =>
          compileSeparator(extension, ChunkSeparator.Slash).map(DefaultChunkKeyEncoding.apply)
        case "v2"  => compileSeparator(extension, ChunkSeparator.Dot).map(V2ChunkKeyEncoding.apply)
        case found => Left(ZarrError.UnsupportedExtension("chunk key encoding", found))

  private def compileSeparator(
      extension: ExtensionMetadata,
      fallback: ChunkSeparator
  ): Either[ZarrError, ChunkSeparator] = extension.configuration.get("separator") match
    case None                       => Right(fallback)
    case Some(JsonValue.Str("/"))   => Right(ChunkSeparator.Slash)
    case Some(JsonValue.Str("."))   => Right(ChunkSeparator.Dot)
    case Some(JsonValue.Str(found)) =>
      Left(
        ZarrError.InvalidMetadata(
          "$.chunk_key_encoding.configuration.separator",
          s"must be '/' or '.', found '$found'"
        )
      )
    case Some(_) =>
      Left(
        ZarrError.InvalidMetadata(
          "$.chunk_key_encoding.configuration.separator",
          "must be a string"
        )
      )

  private def compileStorageTransformers(
      extensions: Vector[ExtensionMetadata]
  ): Either[ZarrError, Unit] =
    extensions.find(_.mustUnderstand) match
      case Some(extension) =>
        Left(ZarrError.UnsupportedExtension("storage transformer", extension.name))
      case None => Right(())

  private def compileLayout(
      extensions: Vector[ExtensionMetadata],
      dataType: DataTypeCapability,
      outerGrid: RegularGrid,
      capabilities: ZarrCapabilities
  ): Either[ZarrError, PhysicalLayout] = extensions.headOption match
    case Some(sharding) if sharding.name == "sharding_indexed" =>
      compileSharding(sharding, extensions.tail, dataType, outerGrid, capabilities)
    case _ =>
      compileCodecs(
        extensions,
        dataType,
        capabilities,
        CodecRepresentation.ArrayValues
      ).flatMap: codecs =>
        codecs.encodedArrayShape(outerGrid.chunkShape).map(_ => PhysicalLayout.Direct(codecs))

  private def compileSharding(
      extension: ExtensionMetadata,
      outerExtensions: Vector[ExtensionMetadata],
      dataType: DataTypeCapability,
      outerGrid: RegularGrid,
      capabilities: ZarrCapabilities
  ): Either[ZarrError, PhysicalLayout] =
    val configuration = extension.configuration
    for
      innerShape <- requiredShape(
        configuration,
        "chunk_shape",
        "$.codecs[0].configuration.chunk_shape"
      )
      shardedGrid <- ShardedGrid(outerGrid, innerShape)
      innerMetadata <- requiredCodecMetadata(
        configuration,
        "codecs",
        "$.codecs[0].configuration.codecs"
      )
      innerCodecs <- compileCodecs(
        innerMetadata,
        dataType,
        capabilities,
        CodecRepresentation.ArrayValues
      )
      _ <- innerCodecs.encodedArrayShape(shardedGrid.innerChunkShape)
      indexMetadata <- requiredCodecMetadata(
        configuration,
        "index_codecs",
        "$.codecs[0].configuration.index_codecs"
      )
      indexCodecs <- compileIndexCodecs(indexMetadata)
      indexLocation <- parseIndexLocation(configuration)
      outerCodecs <- compileCodecs(
        outerExtensions,
        dataType,
        capabilities,
        CodecRepresentation.Bytes
      )
    yield PhysicalLayout.Sharded(
      shardedGrid,
      innerCodecs,
      indexCodecs,
      indexLocation,
      outerCodecs
    )

  private def requiredShape(
      configuration: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Shape] = configuration.get(name) match
    case None => Left(ZarrError.InvalidMetadata(path, "missing required field"))
    case Some(JsonValue.Arr(values)) =>
      val dimensions = Vector.newBuilder[Long]
      var index = 0
      while index < values.length do
        values(index) match
          case JsonValue.Num(number) =>
            number.toLongExact match
              case Right(found) => dimensions += found
              case Left(detail) => return Left(ZarrError.InvalidMetadata(s"$path[$index]", detail))
          case _ => return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be an integer"))
        index += 1
      Shape.from(dimensions.result())
    case Some(_) => Left(ZarrError.InvalidMetadata(path, "must be an array"))

  private def requiredCodecMetadata(
      configuration: JsonObject,
      name: String,
      path: String
  ): Either[ZarrError, Vector[ExtensionMetadata]] = configuration.get(name) match
    case None => Left(ZarrError.InvalidMetadata(path, "missing required field"))
    case Some(JsonValue.Arr(values)) => ZarrMetadata.parseExtensions(values, path)
    case Some(_)                     => Left(ZarrError.InvalidMetadata(path, "must be an array"))

  private def compileIndexCodecs(
      extensions: Vector[ExtensionMetadata]
  ): Either[ZarrError, ShardIndexProgram] = extensions match
    case Vector(bytes, crc) if bytes.name == "bytes" && crc.name == "crc32c" =>
      BuiltInCodecs.bytes.compile(bytes, IndexUInt64DataType) match
        case Left(detail) =>
          Left(
            ZarrError.InvalidMetadata(
              "$.codecs[0].configuration.index_codecs[0]",
              detail
            )
          )
        case Right(found @ BytesCodec(Some(Endianness.Little))) =>
          BuiltInCodecs.crc32c.compile(crc, IndexUInt64DataType) match
            case Left(detail) =>
              Left(
                ZarrError.InvalidMetadata(
                  "$.codecs[0].configuration.index_codecs[1]",
                  detail
                )
              )
            case Right(checksum) => ShardIndexProgram.compile(Vector(found, checksum))
        case Right(_) =>
          Left(
            ZarrError.InvalidMetadata(
              "$.codecs[0].configuration.index_codecs[0]",
              "first supported index codec must be little-endian bytes"
            )
          )
    case _ =>
      Left(
        ZarrError.UnsupportedExtension(
          "shard index codec pipeline",
          extensions.map(_.name).mkString("[", ",", "]")
        )
      )

  private object IndexUInt64DataType extends DataTypeCapability:
    val name = "uint64"
    val scalarKind = ScalarKind.Unsigned64
    def parseFill(value: JsonValue): Either[String, StoredScalar] =
      Left("uint64 is internal to shard index decoding")

  private def parseIndexLocation(
      configuration: JsonObject
  ): Either[ZarrError, IndexLocation] = configuration.get("index_location") match
    case None | Some(JsonValue.Str("end")) => Right(IndexLocation.End)
    case Some(JsonValue.Str("start"))      => Right(IndexLocation.Start)
    case Some(JsonValue.Str(found))        =>
      Left(
        ZarrError.InvalidMetadata(
          "$.codecs[0].configuration.index_location",
          s"must be 'start' or 'end', found '$found'"
        )
      )
    case Some(_) =>
      Left(
        ZarrError.InvalidMetadata(
          "$.codecs[0].configuration.index_location",
          "must be a string"
        )
      )

  private def compileCodecs(
      extensions: Vector[ExtensionMetadata],
      dataType: DataTypeCapability,
      capabilities: ZarrCapabilities,
      initial: CodecRepresentation
  ): Either[ZarrError, CodecProgram] =
    val result = Vector.newBuilder[CompiledCodec]
    var index = 0
    while index < extensions.length do
      val extension = extensions(index)
      capabilities.codec(extension.name) match
        case None if extension.mustUnderstand =>
          return Left(ZarrError.UnsupportedExtension("codec", extension.name))
        case None             => ()
        case Some(capability) =>
          capability.compile(extension, dataType) match
            case Left(detail) =>
              return Left(ZarrError.InvalidMetadata(s"$$.codecs[$index]", detail))
            case Right(codec) =>
              result += codec
      index += 1
    CodecProgram.compile(initial, result.result())
