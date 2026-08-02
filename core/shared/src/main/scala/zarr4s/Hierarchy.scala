package zarr4s

enum ConsolidationMode:
  case Prefer
  case Require
  case Ignore

enum NodeKind:
  case Group
  case Array

final case class HierarchyLimits(
    maxConsolidatedEntries: Int = 300000,
    maxConsolidatedNodes: Int = 100000,
    maxDiscoveryEntries: Int = 300000,
    maxDiscoveryNodes: Int = 100000,
    maxDiscoveryDepth: Int = 128,
    maxDiscoveryMetadataReads: Int = 10000
):
  require(maxConsolidatedEntries >= 0, "maxConsolidatedEntries must be non-negative")
  require(maxConsolidatedNodes >= 0, "maxConsolidatedNodes must be non-negative")
  require(maxDiscoveryEntries >= 0, "maxDiscoveryEntries must be non-negative")
  require(maxDiscoveryNodes >= 0, "maxDiscoveryNodes must be non-negative")
  require(maxDiscoveryDepth >= 0, "maxDiscoveryDepth must be non-negative")
  require(maxDiscoveryMetadataReads >= 0, "maxDiscoveryMetadataReads must be non-negative")

final case class HierarchyEntry(path: ZarrPath, kind: NodeKind, format: ZarrFormat)

private[zarr4s] enum HierarchyDocument:
  case V3Group(metadata: GroupMetadata)
  case V3Array(metadata: ArrayMetadata)
  case V2Group(metadata: GroupMetadata)
  case V2Array(metadata: V2ArrayMetadata)

  def format: ZarrFormat = this match
    case V3Group(_) | V3Array(_) => ZarrFormat.V3
    case V2Group(_) | V2Array(_) => ZarrFormat.V2

  def kind: NodeKind = this match
    case V3Group(_) | V2Group(_) => NodeKind.Group
    case V3Array(_) | V2Array(_) => NodeKind.Array

  def groupMetadata: Either[ZarrError, GroupMetadata] = this match
    case V3Group(found) => Right(found)
    case V2Group(found) => Right(found)
    case _              => Left(ZarrError.UnsupportedNodeType("array"))

  def arrayDescriptor(capabilities: ZarrCapabilities): Either[ZarrError, ArrayDescriptor] =
    this match
      case V3Array(found) => ArrayDescriptor.compile(found, capabilities)
      case V2Array(found) => V2ArrayDescriptor.compile(found, capabilities)
      case _              => Left(ZarrError.UnsupportedNodeType("group"))

private[zarr4s] final class HierarchyIndex private (
    private val documents: Map[String, HierarchyDocument]
):
  def document(path: ZarrPath): Option[HierarchyDocument] = documents.get(path.value)

  def entries: Vector[HierarchyEntry] = documents.iterator
    .map: (path, document) =>
      HierarchyEntry(ZarrPath.unsafe(path), document.kind, document.format)
    .toVector
    .sortBy(_.path.value)

  def children(path: ZarrPath): Vector[HierarchyEntry] =
    val prefix = if path.value.isEmpty then "" else s"${path.value}/"
    entries.filter: entry =>
      if entry.path == path || !entry.path.value.startsWith(prefix) then false
      else !entry.path.value.drop(prefix.length).contains('/')

private[zarr4s] object HierarchyIndex:
  private[zarr4s] final case class ListedNode(
      path: ZarrPath,
      v3: Option[StoreKey],
      v2Group: Option[StoreKey],
      v2Array: Option[StoreKey],
      v2Attributes: Option[StoreKey]
  )

  def listedNodes(
      base: ZarrPath,
      keys: Vector[StoreKey],
      limits: HierarchyLimits
  ): Either[ZarrError, Vector[ListedNode]] =
    if keys.length > limits.maxDiscoveryEntries then
      Left(
        ZarrError.ResourceLimit(
          "hierarchy discovery listing entries",
          limits.maxDiscoveryEntries,
          keys.length
        )
      )
    else
      val prefix = if base.value.isEmpty then "" else s"${base.value}/"
      val candidates = scala.collection.mutable.Map.empty[String, ListedNode]
      var index = 0
      while index < keys.length do
        val key = keys(index)
        val value = key.value
        if !prefix.isEmpty && !value.startsWith(prefix) then
          return Left(
            ZarrError.InvalidMetadata(
              "$.listing",
              s"lister returned key outside prefix '${base.value}': $value"
            )
          )
        val relative = if prefix.isEmpty then value else value.drop(prefix.length)
        val suffix =
          if relative == "zarr.json" || relative.endsWith("/zarr.json") then Some("zarr.json")
          else if relative == ".zgroup" || relative.endsWith("/.zgroup") then Some(".zgroup")
          else if relative == ".zarray" || relative.endsWith("/.zarray") then Some(".zarray")
          else if relative == ".zattrs" || relative.endsWith("/.zattrs") then Some(".zattrs")
          else None
        suffix match
          case None              => ()
          case Some(foundSuffix) =>
            val nodeRelative = relative.dropRight(foundSuffix.length).stripSuffix("/")
            if nodeRelative.nonEmpty then
              val path = base.resolve(nodeRelative) match
                case Left(error)  => return Left(error)
                case Right(found) => found
              val depth = nodeRelative.split('/').length
              if depth > limits.maxDiscoveryDepth then
                return Left(
                  ZarrError.ResourceLimit(
                    "hierarchy discovery depth",
                    limits.maxDiscoveryDepth,
                    depth
                  )
                )
              val existing = candidates.getOrElse(
                path.value,
                ListedNode(path, None, None, None, None)
              )
              val updated = foundSuffix match
                case "zarr.json" if existing.v3.nonEmpty =>
                  return Left(
                    ZarrError.InvalidMetadata(
                      "$.listing",
                      s"duplicate zarr.json for '${path.value}'"
                    )
                  )
                case ".zgroup" if existing.v2Group.nonEmpty =>
                  return Left(
                    ZarrError.InvalidMetadata(
                      "$.listing",
                      s"duplicate .zgroup for '${path.value}'"
                    )
                  )
                case ".zarray" if existing.v2Array.nonEmpty =>
                  return Left(
                    ZarrError.InvalidMetadata(
                      "$.listing",
                      s"duplicate .zarray for '${path.value}'"
                    )
                  )
                case ".zattrs" if existing.v2Attributes.nonEmpty =>
                  return Left(
                    ZarrError.InvalidMetadata(
                      "$.listing",
                      s"duplicate .zattrs for '${path.value}'"
                    )
                  )
                case "zarr.json" => existing.copy(v3 = Some(key))
                case ".zgroup"   => existing.copy(v2Group = Some(key))
                case ".zarray"   => existing.copy(v2Array = Some(key))
                case ".zattrs"   => existing.copy(v2Attributes = Some(key))
                case _           => existing
              candidates.update(path.value, updated)
        index += 1
      val nodes = candidates.values.toVector.sortBy(_.path.value)
      if nodes.length > limits.maxDiscoveryNodes then
        Left(
          ZarrError.ResourceLimit(
            "hierarchy discovery nodes",
            limits.maxDiscoveryNodes,
            nodes.length
          )
        )
      else
        val conflict = nodes.find: node =>
          (node.v3.nonEmpty &&
            (node.v2Group.nonEmpty || node.v2Array.nonEmpty || node.v2Attributes.nonEmpty)) ||
            (node.v2Group.nonEmpty && node.v2Array.nonEmpty)
        conflict match
          case Some(node) =>
            Left(
              ZarrError.InvalidMetadata(
                "$.listing",
                s"conflicting metadata formats for '${node.path.value}'"
              )
            )
          case None
              if nodes
                .exists(node => node.v3.isEmpty && node.v2Group.isEmpty && node.v2Array.isEmpty) =>
            val orphan =
              nodes.find(node => node.v3.isEmpty && node.v2Group.isEmpty && node.v2Array.isEmpty)
            Left(
              ZarrError.InvalidMetadata(
                "$.listing",
                s"orphan v2 attributes for '${orphan.map(_.path.value).getOrElse("")}'"
              )
            )
          case None =>
            val metadataReads = nodes
              .map(node =>
                (if node.v3.nonEmpty || node.v2Group.nonEmpty || node.v2Array.nonEmpty then 1
                 else 0) +
                  (if node.v2Attributes.nonEmpty then 1 else 0)
              )
              .sum
            if metadataReads > limits.maxDiscoveryMetadataReads then
              Left(
                ZarrError.ResourceLimit(
                  "hierarchy discovery metadata reads",
                  limits.maxDiscoveryMetadataReads,
                  metadataReads
                )
              )
            else Right(nodes)

  def discovered(
      base: ZarrPath,
      root: HierarchyDocument,
      documents: Vector[(ZarrPath, HierarchyDocument)],
      limits: HierarchyLimits
  ): Either[ZarrError, HierarchyIndex] =
    val all = scala.collection.mutable.Map(base.value -> root)
    var index = 0
    while index < documents.length do
      val (path, document) = documents(index)
      if path == base || all.contains(path.value) then
        return Left(
          ZarrError.InvalidMetadata("$.listing", s"duplicate hierarchy node '${path.value}'")
        )
      if document.format != root.format then
        return Left(
          ZarrError.InvalidMetadata(
            "$.listing",
            s"mixed metadata formats at '${path.value}'"
          )
        )
      all.update(path.value, document)
      index += 1
    if all.size > limits.maxDiscoveryNodes + 1L then
      Left(
        ZarrError.ResourceLimit(
          "hierarchy discovery nodes",
          limits.maxDiscoveryNodes,
          all.size - 1L
        )
      )
    else
      val paths = all.keysIterator
      while paths.hasNext do
        val pathValue = paths.next()
        if pathValue != base.value then
          val relative =
            if base.value.isEmpty then pathValue else pathValue.drop(base.value.length + 1)
          val parts = relative.split('/')
          var depth = 1
          while depth < parts.length do
            val ancestor =
              if base.value.isEmpty then parts.take(depth).mkString("/")
              else s"${base.value}/${parts.take(depth).mkString("/")}"
            all.get(ancestor) match
              case Some(document) if document.kind == NodeKind.Array =>
                return Left(
                  ZarrError.InvalidMetadata(
                    "$.listing",
                    s"array node '$ancestor' cannot contain descendant '$pathValue'"
                  )
                )
              case _ => ()
            depth += 1
      Right(new HierarchyIndex(all.toMap))

  def v3(
      base: ZarrPath,
      root: GroupMetadata,
      limits: HierarchyLimits
  ): Either[ZarrError, Option[HierarchyIndex]] = root.unknown.get("consolidated_metadata") match
    case None                              => Right(None)
    case Some(JsonValue.Obj(consolidated)) =>
      val kind = consolidated.get("kind") match
        case Some(JsonValue.Str(found)) => found
        case Some(_)                    =>
          return Left(
            ZarrError.InvalidMetadata(
              "$.consolidated_metadata.kind",
              "must be a string"
            )
          )
        case None =>
          return Left(
            ZarrError.InvalidMetadata(
              "$.consolidated_metadata.kind",
              "missing required field"
            )
          )
      val mustUnderstand = consolidated.get("must_understand") match
        case None                        => false
        case Some(JsonValue.Bool(found)) => found
        case Some(_)                     =>
          return Left(
            ZarrError.InvalidMetadata(
              "$.consolidated_metadata.must_understand",
              "must be a boolean"
            )
          )
      if kind != "inline" then
        if mustUnderstand then Left(ZarrError.UnsupportedExtension("consolidated metadata", kind))
        else Right(None)
      else
        consolidated.get("metadata") match
          case Some(JsonValue.Obj(metadata)) =>
            if metadata.fields.length > limits.maxConsolidatedEntries then
              Left(
                ZarrError.ResourceLimit(
                  "consolidated metadata entries",
                  limits.maxConsolidatedEntries,
                  metadata.fields.length
                )
              )
            else
              val documents = scala.collection.mutable.Map.empty[String, HierarchyDocument]
              documents.update(base.value, HierarchyDocument.V3Group(root))
              var index = 0
              while index < metadata.fields.length do
                val (relative, value) = metadata.fields(index)
                if relative.isEmpty then
                  return Left(
                    ZarrError.InvalidMetadata(
                      "$.consolidated_metadata.metadata",
                      "child path must be non-empty"
                    )
                  )
                val path = base.resolve(relative) match
                  case Left(error)  => return Left(error)
                  case Right(found) => found
                value match
                  case JsonValue.Obj(_) =>
                    ZarrMetadata.parse(value.render) match
                      case Left(error)                          => return Left(error)
                      case Right(ZarrNodeMetadata.Group(found)) =>
                        documents.update(path.value, HierarchyDocument.V3Group(found))
                      case Right(ZarrNodeMetadata.Array(found)) =>
                        documents.update(path.value, HierarchyDocument.V3Array(found))
                  case _ =>
                    return Left(
                      ZarrError.InvalidMetadata(
                        s"$$.consolidated_metadata.metadata.${JsonValue.Str(relative).render}",
                        "node metadata must be an object"
                      )
                    )
                index += 1
              if documents.size > limits.maxConsolidatedNodes then
                Left(
                  ZarrError.ResourceLimit(
                    "consolidated metadata nodes",
                    limits.maxConsolidatedNodes,
                    documents.size
                  )
                )
              else Right(Some(new HierarchyIndex(documents.toMap)))
          case Some(_) =>
            Left(
              ZarrError.InvalidMetadata(
                "$.consolidated_metadata.metadata",
                "must be an object"
              )
            )
          case None =>
            Left(
              ZarrError.InvalidMetadata(
                "$.consolidated_metadata.metadata",
                "missing required field"
              )
            )
    case Some(_) => Left(ZarrError.InvalidMetadata("$.consolidated_metadata", "must be an object"))

  def v2(
      base: ZarrPath,
      input: String,
      limits: HierarchyLimits,
      jsonLimits: JsonLimits = JsonLimits()
  ): Either[ZarrError, HierarchyIndex] =
    val root = JsonParser
      .parse(input, jsonLimits)
      .left
      .map(ZarrError.InvalidJson.apply)
      .flatMap:
        case JsonValue.Obj(found) => Right(found)
        case _ => Left(ZarrError.InvalidMetadata("$.zmetadata", "must be an object"))
    root.flatMap: consolidated =>
      for
        version <- consolidated.get("zarr_consolidated_format") match
          case Some(JsonValue.Num(number)) =>
            number.toLongExact.left.map: detail =>
              ZarrError.InvalidMetadata("$.zmetadata.zarr_consolidated_format", detail)
          case Some(_) =>
            Left(
              ZarrError.InvalidMetadata(
                "$.zmetadata.zarr_consolidated_format",
                "must be an integer"
              )
            )
          case None =>
            Left(
              ZarrError.InvalidMetadata(
                "$.zmetadata.zarr_consolidated_format",
                "missing required field"
              )
            )
        _ <-
          if version == 1L then Right(())
          else
            Left(
              ZarrError.InvalidMetadata(
                "$.zmetadata.zarr_consolidated_format",
                s"unsupported consolidated format $version"
              )
            )
        metadata <- consolidated.get("metadata") match
          case Some(JsonValue.Obj(found)) => Right(found)
          case Some(_)                    =>
            Left(ZarrError.InvalidMetadata("$.zmetadata.metadata", "must be an object"))
          case None =>
            Left(ZarrError.InvalidMetadata("$.zmetadata.metadata", "missing required field"))
        _ <-
          if metadata.fields.length <= limits.maxConsolidatedEntries then Right(())
          else
            Left(
              ZarrError.ResourceLimit(
                "consolidated metadata entries",
                limits.maxConsolidatedEntries,
                metadata.fields.length
              )
            )
        index <- compileV2Entries(base, metadata, limits)
      yield index

  private def compileV2Entries(
      base: ZarrPath,
      metadata: JsonObject,
      limits: HierarchyLimits
  ): Either[ZarrError, HierarchyIndex] =
    val arrays = scala.collection.mutable.Map.empty[String, JsonObject]
    val groups = scala.collection.mutable.Map.empty[String, JsonObject]
    val attributes = scala.collection.mutable.Map.empty[String, JsonObject]
    var index = 0
    while index < metadata.fields.length do
      val (key, value) = metadata.fields(index)
      val (path, suffix) = splitV2MetadataKey(key) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      val objectValue = value match
        case JsonValue.Obj(found) => found
        case _                    =>
          return Left(
            ZarrError.InvalidMetadata(
              s"$$.zmetadata.metadata.${JsonValue.Str(key).render}",
              "metadata document must be an object"
            )
          )
      suffix match
        case "zarray" => arrays.update(path, objectValue)
        case "zgroup" => groups.update(path, objectValue)
        case "zattrs" => attributes.update(path, objectValue)
        case _        =>
          return Left(ZarrError.InvalidMetadata("$.zmetadata.metadata", s"unsupported key '$key'"))
      index += 1

    val arrayPaths = arrays.keysIterator
    while arrayPaths.hasNext do
      val path = arrayPaths.next()
      if groups.contains(path) then
        return Left(
          ZarrError.InvalidMetadata("$.zmetadata.metadata", s"path '$path' is both array and group")
        )

    val inferredGroups = scala.collection.mutable.HashSet.empty[String]
    if !arrays.contains("") then inferredGroups += ""
    (arrays.keysIterator ++ groups.keysIterator).foreach: path =>
      val segments = if path.isEmpty then Vector.empty else path.split('/').toVector
      var length = 1
      while length < segments.length do
        inferredGroups += segments.take(length).mkString("/")
        length += 1
    inferredGroups ++= groups.keys

    val conflictingAncestors = arrays.keysIterator
    while conflictingAncestors.hasNext do
      val path = conflictingAncestors.next()
      if inferredGroups.contains(path) then
        return Left(
          ZarrError.InvalidMetadata(
            "$.zmetadata.metadata",
            s"array path '$path' is also required as an ancestor group"
          )
        )

    val attributePaths = attributes.keysIterator
    while attributePaths.hasNext do
      val path = attributePaths.next()
      if !arrays.contains(path) && !inferredGroups.contains(path) then
        return Left(
          ZarrError.InvalidMetadata("$.zmetadata.metadata", s"orphan attributes at '$path'")
        )

    val documents = scala.collection.mutable.Map.empty[String, HierarchyDocument]
    val arrayEntries = arrays.iterator
    while arrayEntries.hasNext do
      val (relative, array) = arrayEntries.next()
      val attrs = attributes.getOrElse(relative, JsonObject.empty)
      val parsed = V2Metadata.parseArrayObject(array, attrs) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      val path = base.resolve(relative) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      documents.update(path.value, HierarchyDocument.V2Array(parsed))
    val groupEntries = inferredGroups.iterator
    while groupEntries.hasNext do
      val relative = groupEntries.next()
      val attrs = attributes.getOrElse(relative, JsonObject.empty)
      val parsed = groups.get(relative) match
        case None        => GroupMetadata(attrs, JsonObject.empty)
        case Some(group) =>
          V2Metadata.parseGroupObject(group, attrs) match
            case Left(error)  => return Left(error)
            case Right(found) => found
      val path = base.resolve(relative) match
        case Left(error)  => return Left(error)
        case Right(found) => found
      documents.update(path.value, HierarchyDocument.V2Group(parsed))
    if documents.size > limits.maxConsolidatedNodes then
      Left(
        ZarrError.ResourceLimit(
          "consolidated metadata nodes",
          limits.maxConsolidatedNodes,
          documents.size
        )
      )
    else Right(new HierarchyIndex(documents.toMap))

  private def splitV2MetadataKey(key: String): Either[ZarrError, (String, String)] =
    val marker = key.lastIndexOf("/.")
    val (path, suffix) =
      if key.startsWith(".") && marker < 0 then "" -> key.drop(1)
      else if marker >= 0 then key.take(marker) -> key.drop(marker + 2)
      else
        return Left(
          ZarrError.InvalidMetadata("$.zmetadata.metadata", s"invalid metadata key '$key'")
        )
    val validated = if path.isEmpty then Right(ZarrPath.root) else ZarrPath(path)
    validated.map(_ => path -> suffix)
