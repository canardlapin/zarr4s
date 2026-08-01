package zarr4s.codec.blosc

import zarr4s.*

object ZstdFixtures:
  val v3ArrayZstd: String =
    """{"shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"zstd","configuration":{"level":3,"checksum":false}}],"attributes":{},"dimension_names":["y","x"],"zarr_format":3,"node_type":"array","storage_transformers":[]}"""

  val v2ArrayZstd: String =
    """{"zarr_format":2,"shape":[2,3],"chunks":[2,3],"dtype":"<i2","compressor":{"id":"zstd","level":3,"checksum":false},"fill_value":0,"order":"C","filters":null}"""

  val directDecodedChunk: OwnedBytes = hex("0100feff2c0104000500faff")

  /** Six little-endian int16 values compressed by Node's zstd implementation. */
  val directZstdChunk: OwnedBytes =
    hex("28b52ffd200c6100000100feff2c0104000500faff")

  private def hex(value: String): OwnedBytes =
    require(value.length % 2 == 0, "hex fixture must contain complete bytes")
    val bytes = new Array[Byte](value.length / 2)
    var index = 0
    while index < bytes.length do
      bytes(index) = Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16).toByte
      index += 1
    OwnedBytes.copyOf(bytes)
