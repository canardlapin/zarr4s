package zarr4s

object ZarrMetadataRenderer:
  def group(metadata: GroupMetadata): Either[ZarrError, String] =
    for
      known <- JsonObject
        .from(
          Vector(
            "zarr_format" -> number(3L),
            "node_type" -> JsonValue.Str("group"),
            "attributes" -> JsonValue.Obj(metadata.attributes)
          )
        )
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
      merged <- JsonObject
        .from(known.fields ++ metadata.unknown.fields.filterNot: (name, _) =>
          known.contains(name))
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
    yield JsonValue.Obj(merged).render

  def array(descriptor: ArrayDescriptor): Either[ZarrError, String] =
    for
      chunkGrid <- renderExtension(
        "regular",
        Vector(
          "chunk_shape" -> longArray(descriptor.grid.chunkShape.toVector)
        )
      )
      chunkKey <- renderExtension(
        descriptor.chunkKeyEncoding.name,
        Vector(
          "separator" -> JsonValue.Str(descriptor.chunkKeyEncoding.separator match
            case ChunkSeparator.Slash => "/"
            case ChunkSeparator.Dot   => ".")
        )
      )
      codecs <- layoutCodecs(descriptor.layout)
      known <- JsonObject
        .from(
          Vector(
            "zarr_format" -> number(3L),
            "node_type" -> JsonValue.Str("array"),
            "shape" -> longArray(descriptor.shape.toVector),
            "data_type" -> JsonValue.Str(descriptor.dataType.name),
            "chunk_grid" -> chunkGrid,
            "chunk_key_encoding" -> chunkKey,
            "fill_value" -> fillValue(descriptor.fillValue),
            "codecs" -> JsonValue.Arr(codecs),
            "attributes" -> JsonValue.Obj(descriptor.attributes),
            "storage_transformers" -> JsonValue.Arr(Vector.empty)
          ) ++ descriptor.dimensionNames.toVector.map: names =>
            "dimension_names" -> JsonValue.Arr(names.map:
              case None       => JsonValue.Null
              case Some(name) => JsonValue.Str(name))
        )
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
      merged <- JsonObject
        .from(known.fields ++ descriptor.unknown.fields.filterNot: (name, _) =>
          known.contains(name))
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
    yield JsonValue.Obj(merged).render

  private def layoutCodecs(layout: PhysicalLayout): Either[ZarrError, Vector[JsonValue]] =
    layout match
      case PhysicalLayout.Direct(codecs) => compiledCodecs(codecs)
      case PhysicalLayout.Sharded(sharded, innerCodecs, indexCodecs, location, outerCodecs) =>
        for
          inner <- compiledCodecs(innerCodecs)
          index <- compiledCodecs(indexCodecs.codecs)
          outer <- compiledCodecs(outerCodecs)
          sharding <- renderExtension(
            "sharding_indexed",
            Vector(
              "chunk_shape" -> longArray(sharded.innerChunkShape.toVector),
              "codecs" -> JsonValue.Arr(inner),
              "index_codecs" -> JsonValue.Arr(index),
              "index_location" -> JsonValue.Str(location match
                case IndexLocation.Start => "start"
                case IndexLocation.End   => "end")
            )
          )
        yield sharding +: outer

  private def compiledCodecs(codecs: CodecProgram): Either[ZarrError, Vector[JsonValue]] =
    val result = Vector.newBuilder[JsonValue]
    var index = 0
    while index < codecs.stages.length do
      val codec = codecs.stages(index)
      val rendered = renderExtension(codec.name, codec.configuration.fields)
      rendered match
        case Left(error)  => return Left(error)
        case Right(value) => result += value
      index += 1
    Right(result.result())

  private def renderExtension(
      name: String,
      configuration: Vector[(String, JsonValue)]
  ): Either[ZarrError, JsonValue] =
    JsonObject
      .from(configuration)
      .left
      .map: detail =>
        ZarrError.InvalidMetadata("$.configuration", detail)
      .flatMap: config =>
        val fields = Vector.newBuilder[(String, JsonValue)]
        fields += "name" -> JsonValue.Str(name)
        if configuration.nonEmpty then fields += "configuration" -> JsonValue.Obj(config)
        JsonObject
          .from(fields.result())
          .left
          .map: detail =>
            ZarrError.InvalidMetadata("$.extension", detail)
          .map(JsonValue.Obj.apply)

  private def fillValue(fill: StoredScalar): JsonValue = fill match
    case StoredScalar.Boolean(value)          => JsonValue.Bool(value)
    case StoredScalar.Integral(value)         => number(value)
    case StoredScalar.UnsignedIntegral(value) =>
      JsonValue.Num(JsonNumber.unsafe(value.toString))
    case StoredScalar.Floating(value) if value.isNaN                      => JsonValue.Str("NaN")
    case StoredScalar.Floating(value) if value == Double.PositiveInfinity =>
      JsonValue.Str("Infinity")
    case StoredScalar.Floating(value) if value == Double.NegativeInfinity =>
      JsonValue.Str("-Infinity")
    case StoredScalar.Floating(value) =>
      JsonValue.Num(JsonNumber.unsafe(java.lang.Double.toString(value)))
    case StoredScalar.FloatingBits(hex) => JsonValue.Str(hex)

  private def longArray(values: Vector[Long]): JsonValue =
    JsonValue.Arr(values.map(number))

  private def number(value: Long): JsonValue =
    JsonValue.Num(JsonNumber.unsafe(value.toString))
