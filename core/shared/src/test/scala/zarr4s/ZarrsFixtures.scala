package zarr4s

/** Fixture written by the independent zarrs Rust implementation.
  *
  * Source: zarrs commit cf8209811f5937cbe4594a7a3445b95c9d35872c,
  * `zarrs/tests/data/sharded_array_write_read.zarr/group/array`.
  */
object ZarrsFixtures:
  val sourceCommit = "cf8209811f5937cbe4594a7a3445b95c9d35872c"

  val shardedMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[8,8],"data_type":"uint16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,8]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"sharding_indexed","configuration":{"chunk_shape":[4,4],"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":5}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"crc32c"}],"index_location":"end"}}],"attributes":{"_zarrs":{"description":"This array was created with zarrs","repository":"https://github.com/LDeakin/zarrs","version":"0.15.0"}},"dimension_names":["y","x"]}"""

  val shard0: OwnedBytes = ZarrBinaryFixtures.hex(
    "1f8b08000000000000ff6361606560636067e061e065e063e06710611065" +
      "10631067906190659063906700006f20cac1200000001f8b080000000000" +
      "00ff6360606460626066e060e064e062e066106010641062106690609064" +
      "906290660000e9a3c72e2000000034000000000000003400000000000000" +
      "0000000000000000340000000000000074c891c4"
  )

  val shard1: OwnedBytes = ZarrBinaryFixtures.hex(
    "1f8b08000000000000ff5361506550635067d061d065d063d06730613065" +
      "30633067b061b065b063b06700005b2562d4200000001f8b080000000000" +
      "00ff5360506450625066d060d064d062d0663060306430623066b060b064" +
      "b062b0660000dda66f3b2000000034000000000000003400000000000000" +
      "0000000000000000340000000000000074c891c4"
  )

  def objects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> OwnedBytes.copyOf(shardedMetadata.iterator.map(_.toByte).toArray),
    "c/0/0" -> shard0,
    "c/1/0" -> shard1
  )
