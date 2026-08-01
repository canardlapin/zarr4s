package zarr4s

/** Binary objects emitted by Zarr-Python 3.2.1. */
object ZarrBinaryFixtures:
  val directGzipMetadata =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[2,3]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"},{"configuration":{"level":1},"name":"gzip"}],"data_type":"int16","dimension_names":["y","x"],"fill_value":0,"node_type":"array","shape":[2,3],"storage_transformers":[],"zarr_format":3}"""

  val directGzipChunk: OwnedBytes = hex(
    "1f8b08005c125e6a04ff6364f8f75f8791858195e1d77f0052120ac00c000000"
  )

  /** Six little-endian int16 values compressed by Python zlib at level 1. */
  val directZlibChunk: OwnedBytes = hex(
    "78016364f8f75f8791858195e1d77f001781042e"
  )

  /** Six little-endian int16 values transformed by numcodecs Shuffle(elementsize=2). */
  val directShuffledChunk: OwnedBytes = hex("01fe2c0405fa00ff010000ff")

  val directDecodedChunk: OwnedBytes = hex("0100feff2c0104000500faff")

  val shardedStartMetadata =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[4,4]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"chunk_shape":[2,2],"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"index_codecs":[{"configuration":{"endian":"little"},"name":"bytes"},{"name":"crc32c"}],"index_location":"start"},"name":"sharding_indexed"}],"data_type":"int16","dimension_names":["y","x"],"fill_value":0,"node_type":"array","shape":[4,4],"storage_transformers":[],"zarr_format":3}"""

  val shardedStartObject: OwnedBytes = hex(
    "44000000000000000800000000000000" +
      "ffffffffffffffffffffffffffffffff" +
      "ffffffffffffffffffffffffffffffff" +
      "4c000000000000000800000000000000" +
      "a45840a0" +
      "01000200030004000d000e000f001000"
  )

  val shardedEndMetadata: String =
    shardedStartMetadata.replace("\"index_location\":\"start\"", "\"index_location\":\"end\"")

  val shardedEndObject: OwnedBytes = hex(
    "01000200030004000d000e000f001000" +
      "00000000000000000800000000000000" +
      "ffffffffffffffffffffffffffffffff" +
      "ffffffffffffffffffffffffffffffff" +
      "08000000000000000800000000000000" +
      "6729498b"
  )

  def hex(value: String): OwnedBytes =
    require(value.length % 2 == 0, "hex fixture must contain complete bytes")
    val bytes = new Array[Byte](value.length / 2)
    var index = 0
    while index < bytes.length do
      bytes(index) = Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16).toByte
      index += 1
    OwnedBytes.copyOf(bytes)
