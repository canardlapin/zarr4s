package zarr4s

enum ZarrError:
  case InvalidShape(detail: String)
  case InvalidCoordinate(detail: String)
  case RankMismatch(expected: Int, actual: Int, context: String)
  case OutOfBounds(detail: String)
  case ArithmeticOverflow(context: String)
  case InvalidGrid(detail: String)
  case InvalidSelection(detail: String)
  case InvalidJson(error: JsonError)
  case InvalidMetadata(path: String, detail: String)
  case UnsupportedVersion(version: Long)
  case UnsupportedNodeType(nodeType: String)
  case UnsupportedDataType(name: String)
  case UnsupportedExtension(kind: String, name: String)
  case InvalidCodecChain(detail: String)
  case InvalidCodecRuntime(detail: String)
  case StoreFailure(error: StoreError)
  case CodecFailure(error: CodecError)
  case UnsupportedRead(detail: String)
  case UnsupportedWrite(detail: String)
  case WriteFailure(detail: String)
  case ResourceLimit(resource: String, limit: Long, requested: Long)

  def message: String = this match
    case InvalidShape(detail)                    => s"invalid shape: $detail"
    case InvalidCoordinate(detail)               => s"invalid coordinate: $detail"
    case RankMismatch(expected, actual, context) =>
      s"rank mismatch for $context: expected $expected, found $actual"
    case OutOfBounds(detail)              => s"out of bounds: $detail"
    case ArithmeticOverflow(context)      => s"arithmetic overflow while computing $context"
    case InvalidGrid(detail)              => s"invalid chunk grid: $detail"
    case InvalidSelection(detail)         => s"invalid selection: $detail"
    case InvalidJson(error)               => s"invalid JSON: ${error.message}"
    case InvalidMetadata(path, detail)    => s"invalid Zarr metadata at $path: $detail"
    case UnsupportedVersion(version)      => s"unsupported Zarr format version $version"
    case UnsupportedNodeType(nodeType)    => s"unsupported Zarr node type '$nodeType'"
    case UnsupportedDataType(name)        => s"unsupported Zarr data type '$name'"
    case UnsupportedExtension(kind, name) => s"unsupported $kind extension '$name'"
    case InvalidCodecChain(detail)        => s"invalid codec chain: $detail"
    case InvalidCodecRuntime(detail)      => s"invalid codec runtime: $detail"
    case StoreFailure(error)              => error.message
    case CodecFailure(error)              => error.message
    case UnsupportedRead(detail)          => s"unsupported read: $detail"
    case UnsupportedWrite(detail)         => s"unsupported write: $detail"
    case WriteFailure(detail)             => s"write failed: $detail"
    case ResourceLimit(resource, limit, requested) =>
      s"$resource limit exceeded: limit $limit, requested $requested"
