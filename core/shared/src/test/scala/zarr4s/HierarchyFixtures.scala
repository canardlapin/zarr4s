package zarr4s

object HierarchyFixtures:
  val v2ArrayC: String =
    """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"<i2","compressor":null,"fill_value":-1,"order":"C","filters":null}"""

  val v2ArrayF: String =
    """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":">i2","compressor":null,"fill_value":-1,"order":"F","filters":[],"dimension_separator":"/"}"""

  val v2ArrayGzip: String =
    """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"<i2","compressor":{"id":"gzip","level":1},"fill_value":0,"order":"C","filters":null}"""

  val v2Group: String = """{"zarr_format":2}"""

  val v2Consolidated: String =
    s"""{"zarr_consolidated_format":1,"metadata":{".zgroup":$v2Group,".zattrs":{"title":"root"},"bold/.zarray":$v2ArrayC,"bold/.zattrs":{"_ARRAY_DIMENSIONS":["y","x"]},"derived/mask/.zarray":{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"|u1","compressor":null,"fill_value":0,"order":"C","filters":null}}}"""

  val v3Array: String =
    """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":-1,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  val v3Mask: String =
    """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes"}],"attributes":{},"storage_transformers":[]}"""

  val v3Group: String =
    """{"zarr_format":3,"node_type":"group","attributes":{}}"""

  val v3Consolidated: String =
    s"""{"zarr_format":3,"node_type":"group","attributes":{"title":"root"},"consolidated_metadata":{"kind":"inline","must_understand":false,"metadata":{"bold":$v3Array,"derived":$v3Group,"derived/mask":$v3Mask}}}"""

  val int16LittleChunk: OwnedBytes = OwnedBytes.copyOf(
    Array[Byte](
      1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0
    )
  )

  val int16BigFortranChunk: OwnedBytes = OwnedBytes.copyOf(
    Array[Byte](
      0, 1, 0, 4, 0, 2, 0, 5, 0, 3, 0, 6
    )
  )

  def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)
