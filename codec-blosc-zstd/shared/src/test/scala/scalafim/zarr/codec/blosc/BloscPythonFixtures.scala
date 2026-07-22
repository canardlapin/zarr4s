package scalafim.zarr.codec.blosc

import scalafim.zarr.*

/** Binary objects emitted by Zarr-Python 3.2.1 with numcodecs 0.16.3. */
object BloscPythonFixtures:
  val directMetadata =
    """{"shape":[2,3],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0.0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"blosc","configuration":{"typesize":4,"cname":"zstd","clevel":5,"shuffle":"shuffle","blocksize":0}}],"attributes":{},"dimension_names":["y","x"],"zarr_format":3,"node_type":"array","storage_transformers":[]}"""

  val directChunk: OwnedBytes = hex(
    "02019304180000001800000028000000" +
      "0000a03f000020c000009643000090400000b8400000c0c0"
  )

  val int16Metadata =
    """{"shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"blosc","configuration":{"typesize":2,"cname":"zstd","clevel":5,"shuffle":"shuffle","blocksize":0}}],"attributes":{},"dimension_names":["y","x"],"zarr_format":3,"node_type":"array","storage_transformers":[]}"""

  val int16Chunk: OwnedBytes = hex(
    "020193020c0000000c0000001c0000000100feff2c0104000500faff"
  )

  val shardedMetadata =
    """{"shape":[4,4],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0.0,"codecs":[{"name":"sharding_indexed","configuration":{"chunk_shape":[2,2],"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"blosc","configuration":{"typesize":4,"cname":"zstd","clevel":5,"shuffle":"shuffle","blocksize":0}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"crc32c"}],"index_location":"start"}}],"attributes":{},"dimension_names":["y","x"],"zarr_format":3,"node_type":"array","storage_transformers":[]}"""

  val shardedObject: OwnedBytes = hex(
    "44000000000000002000000000000000" +
      "84000000000000002000000000000000" +
      "64000000000000002000000000000000" +
      "a4000000000000002000000000000000" +
      "123212e3" +
      "020193041000000010000000200000000000803f000000400000a0400000c040" +
      "0201930410000000100000002000000000001041000020410000504100006041" +
      "0201930410000000100000002000000000004040000080400000e04000000041" +
      "0201930410000000100000002000000000003041000040410000704100008041"
  )

  val directValues: Vector[Float] =
    Vector(1.25f, -2.5f, 300.0f, 4.5f, 5.75f, -6.0f)

  val int16Values: Vector[Short] =
    Vector[Short](1, -2, 300, 4, 5, -6)

  val shardedValues: Vector[Float] =
    (1 to 16).map(_.toFloat).toVector

  def directObjects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> utf8(directMetadata),
    "c/0/0" -> directChunk
  )

  def shardedObjects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> utf8(shardedMetadata),
    "c/0/0" -> shardedObject
  )

  def int16Objects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> utf8(int16Metadata),
    "c/0/0" -> int16Chunk
  )

  def descriptor(metadata: String): Either[ZarrError, ArrayDescriptor] =
    ZarrMetadata.parse(metadata).flatMap:
      case ZarrNodeMetadata.Array(array) =>
        ArrayDescriptor.compile(array, BloscZstdProvider.capabilities())
      case ZarrNodeMetadata.Group(_) => Left(ZarrError.UnsupportedNodeType("group"))

  private def utf8(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def hex(value: String): OwnedBytes =
    require(value.length % 2 == 0, "hex fixture must contain complete bytes")
    val bytes = new Array[Byte](value.length / 2)
    var index = 0
    while index < bytes.length do
      bytes(index) = Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16).toByte
      index += 1
    OwnedBytes.copyOf(bytes)
