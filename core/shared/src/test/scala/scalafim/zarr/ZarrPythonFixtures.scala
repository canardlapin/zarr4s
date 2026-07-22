package scalafim.zarr

/** Metadata emitted by Zarr-Python 3.2.1.
  *
  * Each array was created with the bytes serializer, no compressors, and Zarr
  * format 3. Keeping these as source literals makes the same independent
  * fixtures available to MUnit on the JVM and Scala.js.
  */
object ZarrPythonFixtures:
  val rank0 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"data_type":"float32","fill_value":"NaN","node_type":"array","shape":[],"storage_transformers":[],"zarr_format":3}"""

  val rank1 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[3]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"data_type":"float32","dimension_names":["x"],"fill_value":"NaN","node_type":"array","shape":[7],"storage_transformers":[],"zarr_format":3}"""

  val rank2 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[4,3]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"data_type":"float32","dimension_names":["y","x"],"fill_value":"NaN","node_type":"array","shape":[0,9],"storage_transformers":[],"zarr_format":3}"""

  val rank4 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[16,24,32,32]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"data_type":"float32","dimension_names":["time","z","y","x"],"fill_value":"NaN","node_type":"array","shape":[1200,72,96,96],"storage_transformers":[],"zarr_format":3}"""

  val rank5 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[1,16,24,32,32]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"data_type":"float32","dimension_names":["echo","time","z","y","x"],"fill_value":"NaN","node_type":"array","shape":[3,1200,72,96,96],"storage_transformers":[],"zarr_format":3}"""

  val shardedRank2 =
    """{"attributes":{},"chunk_grid":{"configuration":{"chunk_shape":[4,4]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"chunk_shape":[2,2],"codecs":[{"configuration":{"endian":"little"},"name":"bytes"}],"index_codecs":[{"configuration":{"endian":"little"},"name":"bytes"},{"name":"crc32c"}],"index_location":"end"},"name":"sharding_indexed"}],"data_type":"int16","dimension_names":["y","x"],"fill_value":0,"node_type":"array","shape":[8,8],"storage_transformers":[],"zarr_format":3}"""

  val all: Vector[(Int, String)] = Vector(
    0 -> rank0,
    1 -> rank1,
    2 -> rank2,
    4 -> rank4,
    5 -> rank5
  )
