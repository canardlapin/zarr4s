package zarr4s

enum ZarrFormat(val version: Int):
  case V2 extends ZarrFormat(2)
  case V3 extends ZarrFormat(3)

enum V2MemoryOrder:
  case C
  case F

final case class V2ArrayMetadata(
    shape: Vector[Long],
    chunks: Vector[Long],
    dtype: String,
    compressor: Option[JsonObject],
    fillValue: JsonValue,
    order: V2MemoryOrder,
    filters: Vector[JsonObject],
    dimensionSeparator: ChunkSeparator,
    attributes: JsonObject,
    unknown: JsonObject
)

object V2Metadata:
  private val arrayFields = Set(
    "zarr_format",
    "shape",
    "chunks",
    "dtype",
    "compressor",
    "fill_value",
    "order",
    "filters",
    "dimension_separator"
  )

  def parseArray(
      arrayJson: String,
      attributesJson: Option[String] = None,
      limits: JsonLimits = JsonLimits()
  ): Either[ZarrError, V2ArrayMetadata] =
    for
      root <- parseObject(arrayJson, "$.zarray", limits)
      attributes <- parseAttributes(attributesJson, limits)
      metadata <- parseArrayObject(root, attributes)
    yield metadata

  def parseGroup(
      groupJson: String,
      attributesJson: Option[String] = None,
      limits: JsonLimits = JsonLimits()
  ): Either[ZarrError, GroupMetadata] =
    for
      root <- parseObject(groupJson, "$.zgroup", limits)
      attributes <- parseAttributes(attributesJson, limits)
      group <- parseGroupObject(root, attributes)
    yield group

  private[zarr4s] def parseGroupObject(
      root: JsonObject,
      attributes: JsonObject
  ): Either[ZarrError, GroupMetadata] =
    for
      version <- requiredLong(root, "zarr_format", "$.zgroup.zarr_format")
      _ <- if version == 2L then Right(()) else Left(ZarrError.UnsupportedVersion(version))
    yield GroupMetadata(attributes, root.removed(Set("zarr_format")))

  private[zarr4s] def parseArrayObject(
      root: JsonObject,
      attributes: JsonObject
  ): Either[ZarrError, V2ArrayMetadata] =
    for
      version <- requiredLong(root, "zarr_format", "$.zarray.zarr_format")
      _ <- if version == 2L then Right(()) else Left(ZarrError.UnsupportedVersion(version))
      shape <- requiredLongArray(root, "shape", "$.zarray.shape", nonNegative = true)
      chunks <- requiredLongArray(root, "chunks", "$.zarray.chunks", nonNegative = false)
      _ <-
        if chunks.length == shape.length then Right(())
        else Left(ZarrError.RankMismatch(shape.length, chunks.length, "v2 chunk shape"))
      dtype <- requiredString(root, "dtype", "$.zarray.dtype")
      compressor <- optionalObjectOrNull(root, "compressor", "$.zarray.compressor")
      fill <- root
        .get("fill_value")
        .toRight(
          ZarrError.InvalidMetadata(
            "$.zarray.fill_value",
            "missing required field"
          )
        )
      order <- requiredString(root, "order", "$.zarray.order").flatMap:
        case "C"   => Right(V2MemoryOrder.C)
        case "F"   => Right(V2MemoryOrder.F)
        case found =>
          Left(ZarrError.InvalidMetadata("$.zarray.order", s"must be 'C' or 'F', found '$found'"))
      filters <- filters(root)
      separator <- dimensionSeparator(root)
    yield V2ArrayMetadata(
      shape,
      chunks,
      dtype,
      compressor,
      fill,
      order,
      filters,
      separator,
      attributes,
      root.removed(arrayFields)
    )

  private[zarr4s] def parseAttributesObject(
      value: JsonValue,
      path: String
  ): Either[ZarrError, JsonObject] =
    value match
      case JsonValue.Obj(found) => Right(found)
      case _ => Left(ZarrError.InvalidMetadata(path, "attributes must be an object"))

  private def parseAttributes(
      input: Option[String],
      limits: JsonLimits
  ): Either[ZarrError, JsonObject] = input match
    case None        => Right(JsonObject.empty)
    case Some(found) => parseObject(found, "$.zattrs", limits)

  private def parseObject(
      input: String,
      path: String,
      limits: JsonLimits
  ): Either[ZarrError, JsonObject] =
    JsonParser
      .parse(input, limits)
      .left
      .map(ZarrError.InvalidJson.apply)
      .flatMap:
        case JsonValue.Obj(found) => Right(found)
        case _                    => Left(ZarrError.InvalidMetadata(path, "must be an object"))

  private def filters(root: JsonObject): Either[ZarrError, Vector[JsonObject]] =
    root.get("filters") match
      case Some(JsonValue.Null)        => Right(Vector.empty)
      case Some(JsonValue.Arr(values)) =>
        val result = Vector.newBuilder[JsonObject]
        var index = 0
        while index < values.length do
          values(index) match
            case JsonValue.Obj(found) => result += found
            case _                    =>
              return Left(
                ZarrError.InvalidMetadata(s"$$.zarray.filters[$index]", "must be an object")
              )
          index += 1
        Right(result.result())
      case Some(_) =>
        Left(ZarrError.InvalidMetadata("$.zarray.filters", "must be an array or null"))
      case None => Left(ZarrError.InvalidMetadata("$.zarray.filters", "missing required field"))

  private def dimensionSeparator(root: JsonObject): Either[ZarrError, ChunkSeparator] =
    root.get("dimension_separator") match
      case None | Some(JsonValue.Str(".")) => Right(ChunkSeparator.Dot)
      case Some(JsonValue.Str("/"))        => Right(ChunkSeparator.Slash)
      case Some(JsonValue.Str(found))      =>
        Left(
          ZarrError.InvalidMetadata(
            "$.zarray.dimension_separator",
            s"must be '.' or '/', found '$found'"
          )
        )
      case Some(_) =>
        Left(ZarrError.InvalidMetadata("$.zarray.dimension_separator", "must be a string"))

  private def requiredLongArray(
      root: JsonObject,
      field: String,
      path: String,
      nonNegative: Boolean
  ): Either[ZarrError, Vector[Long]] = root.get(field) match
    case Some(JsonValue.Arr(values)) =>
      val result = Vector.newBuilder[Long]
      var index = 0
      while index < values.length do
        values(index) match
          case JsonValue.Num(number) =>
            number.toLongExact match
              case Left(detail) => return Left(ZarrError.InvalidMetadata(s"$path[$index]", detail))
              case Right(found) if nonNegative && found < 0L =>
                return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be non-negative"))
              case Right(found) if !nonNegative && found <= 0L =>
                return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be positive"))
              case Right(found) => result += found
          case _ => return Left(ZarrError.InvalidMetadata(s"$path[$index]", "must be an integer"))
        index += 1
      Right(result.result())
    case Some(_) => Left(ZarrError.InvalidMetadata(path, "must be an array"))
    case None    => Left(ZarrError.InvalidMetadata(path, "missing required field"))

  private def requiredLong(root: JsonObject, field: String, path: String): Either[ZarrError, Long] =
    root.get(field) match
      case Some(JsonValue.Num(number)) =>
        number.toLongExact.left.map(ZarrError.InvalidMetadata(path, _))
      case Some(_) => Left(ZarrError.InvalidMetadata(path, "must be an integer"))
      case None    => Left(ZarrError.InvalidMetadata(path, "missing required field"))

  private def requiredString(
      root: JsonObject,
      field: String,
      path: String
  ): Either[ZarrError, String] =
    root.get(field) match
      case Some(JsonValue.Str(found)) => Right(found)
      case Some(_)                    => Left(ZarrError.InvalidMetadata(path, "must be a string"))
      case None => Left(ZarrError.InvalidMetadata(path, "missing required field"))

  private def optionalObjectOrNull(
      root: JsonObject,
      field: String,
      path: String
  ): Either[ZarrError, Option[JsonObject]] = root.get(field) match
    case Some(JsonValue.Null)       => Right(None)
    case Some(JsonValue.Obj(found)) => Right(Some(found))
    case Some(_) => Left(ZarrError.InvalidMetadata(path, "must be an object or null"))
    case None    => Left(ZarrError.InvalidMetadata(path, "missing required field"))

object V2ArrayDescriptor:
  private final case class DType(name: String, endianness: Option[Endianness], byteWidth: Int)

  def compile(
      metadata: V2ArrayMetadata,
      capabilities: ZarrCapabilities = ZarrCapabilities()
  ): Either[ZarrError, ArrayDescriptor] =
    for
      dtype <- parseDType(metadata.dtype)
      filterCodecs <- filterCodecs(metadata.filters)
      codecs <- codecs(metadata, dtype, filterCodecs)
      chunkGrid <- extension(
        "regular",
        Vector(
          "chunk_shape" -> JsonValue.Arr(metadata.chunks.map(number))
        )
      )
      keyEncoding <- extension(
        "v2",
        Vector(
          "separator" -> JsonValue.Str(metadata.dimensionSeparator match
            case ChunkSeparator.Dot   => "."
            case ChunkSeparator.Slash => "/")
        )
      )
      descriptor <- ArrayDescriptor.compile(
        ArrayMetadata(
          metadata.shape,
          ExtensionMetadata(dtype.name, JsonObject.empty, true, JsonObject.empty),
          chunkGrid,
          keyEncoding,
          metadata.fillValue,
          codecs,
          metadata.attributes,
          dimensionNames(metadata.attributes, metadata.shape.length),
          Vector.empty,
          JsonObject.empty
        ),
        capabilities
      )
    yield descriptor

  private def parseDType(value: String): Either[ZarrError, DType] =
    if value.length < 3 then unsupported(value)
    else
      val byteOrder = value.charAt(0)
      val kind = value.charAt(1)
      val widthText = value.drop(2)
      val width = widthText.toIntOption match
        case Some(found) if found > 0 => found
        case _                        => return unsupported(value)
      val name = (kind, width) match
        case ('b', 1) => Some("bool")
        case ('i', 1) => Some("int8")
        case ('i', 2) => Some("int16")
        case ('i', 4) => Some("int32")
        case ('i', 8) => Some("int64")
        case ('u', 1) => Some("uint8")
        case ('u', 2) => Some("uint16")
        case ('u', 4) => Some("uint32")
        case ('u', 8) => Some("uint64")
        case ('f', 4) => Some("float32")
        case ('f', 8) => Some("float64")
        case _        => None
      name match
        case None => unsupported(value)
        case Some(found) if width == 1 && Set('|', '<', '>').contains(byteOrder) =>
          Right(DType(found, None, width))
        case Some(found) if byteOrder == '<' => Right(DType(found, Some(Endianness.Little), width))
        case Some(found) if byteOrder == '>' => Right(DType(found, Some(Endianness.Big), width))
        case Some(_)                         => unsupported(value)

  private def filterCodecs(
      filters: Vector[JsonObject]
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    val result = Vector.newBuilder[ExtensionMetadata]
    var index = 0
    while index < filters.length do
      val filter = filters(index)
      identifier(filter) match
        case None =>
          return Left(
            ZarrError.InvalidMetadata(
              s"$$.zarray.filters[$index].id",
              "missing string field"
            )
          )
        case Some("shuffle") =>
          result += ExtensionMetadata(
            "shuffle",
            filter.removed(Set("id")),
            true,
            JsonObject.empty
          )
        case Some(found) =>
          return Left(ZarrError.UnsupportedExtension("v2 filter", found))
      index += 1
    Right(result.result())

  private def codecs(
      metadata: V2ArrayMetadata,
      dtype: DType,
      filterCodecs: Vector[ExtensionMetadata]
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    val result = Vector.newBuilder[ExtensionMetadata]
    if metadata.order == V2MemoryOrder.F then
      result += ExtensionMetadata(
        "transpose",
        JsonObject.unsafe(
          Vector(
            "order" -> JsonValue.Arr(
              metadata.shape.indices.reverse.map(index => number(index.toLong)).toVector
            )
          )
        ),
        true,
        JsonObject.empty
      )
    val bytesConfiguration = dtype.endianness match
      case None        => JsonObject.empty
      case Some(found) =>
        JsonObject.unsafe(
          Vector(
            "endian" -> JsonValue.Str(found match
              case Endianness.Little => "little"
              case Endianness.Big    => "big")
          )
        )
    result += ExtensionMetadata("bytes", bytesConfiguration, true, JsonObject.empty)
    filterCodecs.foreach(result += _)
    metadata.compressor match
      case None             => Right(result.result())
      case Some(compressor) =>
        compressorExtension(compressor, dtype).map: extension =>
          result += extension
          result.result()

  private def compressorExtension(
      compressor: JsonObject,
      dtype: DType
  ): Either[ZarrError, ExtensionMetadata] = identifier(compressor) match
    case None => Left(ZarrError.InvalidMetadata("$.zarray.compressor.id", "missing string field"))
    case Some("gzip") =>
      Right(
        ExtensionMetadata(
          "gzip",
          compressor.removed(Set("id")),
          true,
          JsonObject.empty
        )
      )
    case Some("zlib") =>
      Right(
        ExtensionMetadata(
          "zlib",
          compressor.removed(Set("id")),
          true,
          JsonObject.empty
        )
      )
    case Some("zstd") =>
      Right(
        ExtensionMetadata(
          "zstd",
          compressor.removed(Set("id")),
          true,
          JsonObject.empty
        )
      )
    case Some("crc32c") =>
      Right(ExtensionMetadata("crc32c", JsonObject.empty, true, JsonObject.empty))
    case Some("blosc") => translateBlosc(compressor, dtype)
    case Some(found)   => Left(ZarrError.UnsupportedExtension("v2 compressor", found))

  private def translateBlosc(
      compressor: JsonObject,
      dtype: DType
  ): Either[ZarrError, ExtensionMetadata] =
    val shuffle = compressor.get("shuffle") match
      case Some(JsonValue.Num(number)) =>
        number.toLongExact.flatMap:
          case 0L    => Right("noshuffle")
          case 1L    => Right("shuffle")
          case 2L    => Right("bitshuffle")
          case found => Left(s"unsupported v2 blosc shuffle $found")
      case Some(_) => Left("v2 blosc shuffle must be an integer")
      case None    => Right("noshuffle")
    shuffle.left
      .map(detail => ZarrError.InvalidMetadata("$.zarray.compressor.shuffle", detail))
      .map: found =>
        val withoutLegacy = compressor.removed(Set("id", "shuffle", "typesize", "blocksize"))
        val fields = withoutLegacy.fields ++ Vector(
          "shuffle" -> JsonValue.Str(found),
          "typesize" -> compressor.get("typesize").getOrElse(number(dtype.byteWidth.toLong)),
          "blocksize" -> compressor.get("blocksize").getOrElse(number(0L))
        )
        ExtensionMetadata("blosc", JsonObject.unsafe(fields), true, JsonObject.empty)

  private def dimensionNames(
      attributes: JsonObject,
      rank: Int
  ): Option[Vector[Option[String]]] = attributes.get("_ARRAY_DIMENSIONS") match
    case Some(JsonValue.Arr(values))
        if values.length == rank && values.forall(_.isInstanceOf[JsonValue.Str]) =>
      Some(values.map:
        case JsonValue.Str(found) => Some(found)
        case _                    => None)
    case _ => None

  private def identifier(value: JsonObject): Option[String] = value.get("id") match
    case Some(JsonValue.Str(found)) => Some(found)
    case _                          => None

  private def extension(
      name: String,
      fields: Vector[(String, JsonValue)]
  ): Either[ZarrError, ExtensionMetadata] =
    JsonObject
      .from(fields)
      .left
      .map(detail => ZarrError.InvalidMetadata("$.configuration", detail))
      .map: config =>
        ExtensionMetadata(name, config, true, JsonObject.empty)

  private def number(value: Long): JsonValue = JsonValue.Num(JsonNumber.unsafe(value.toString))

  private def unsupported(value: String): Left[ZarrError, Nothing] =
    Left(ZarrError.UnsupportedDataType(s"v2 dtype $value"))
