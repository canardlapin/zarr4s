package scalafim.zarr

/** Direct Zarr v3 fixture written by zarr-java 0.1.3.
  *
  * Generated with `tools/verify_zarr_java_oracle.py write-fixture`. The raw
  * metadata and chunk hashes make the independent writer provenance
  * inspectable without adding zarr-java to the Scala build.
  */
object ZarrJavaFixtures:
  val sourceVersion = "0.1.3"
  val metadataSha256 = "c8ea994472c18c2de4e29bfb90014a7b38fd1af9967e487c5128ced632435741"
  val chunkSha256 = "d7aad1b09aa52b270ececfc54c2d98cd1fcb7572b284e7d12d8211c501812988"

  val metadata =
    """{
      |  "zarr_format" : 3,
      |  "node_type" : "array",
      |  "shape" : [ 2, 3 ],
      |  "data_type" : "int16",
      |  "chunk_grid" : {
      |    "name" : "regular",
      |    "configuration" : {
      |      "chunk_shape" : [ 2, 3 ]
      |    }
      |  },
      |  "chunk_key_encoding" : {
      |    "name" : "default",
      |    "configuration" : {
      |      "separator" : "/"
      |    }
      |  },
      |  "fill_value" : 0,
      |  "codecs" : [ {
      |    "name" : "bytes",
      |    "configuration" : {
      |      "endian" : "little"
      |    }
      |  } ],
      |  "dimension_names" : [ "y", "x" ],
      |  "attributes" : { },
      |  "storage_transformers" : [ ]
      |}""".stripMargin

  val chunk: OwnedBytes = ZarrBinaryFixtures.hex("0100feff2c0104000500faff")

  def objects: Map[String, OwnedBytes] = Map(
    "zarr.json" -> OwnedBytes.copyOf(metadata.iterator.map(_.toByte).toArray),
    "c/0/0" -> chunk
  )
