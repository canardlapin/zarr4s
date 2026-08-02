package zarr4s

class ShardingSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def count(value: Long): ByteCount = zvalue(ByteCount(value))

  test("little-endian CRC32C shard index round-trips the normative 2 by 2 shape"):
    val shape = zvalue(Shape(2L, 2L))
    val entries = Vector(
      ShardIndexEntry.Present(68L, count(11L)),
      ShardIndexEntry.Fill,
      ShardIndexEntry.Present(79L, count(7L)),
      ShardIndexEntry.Present(86L, count(13L))
    )
    val index = zvalue(ShardIndex(shape, entries))
    val encoded = zvalue(ShardIndexCodec.encode(index))
    assertEquals(encoded.length, 68)
    val decoded = zvalue(ShardIndexCodec.decode(encoded, shape))
    assertEquals(decoded.entries, entries)
    assertEquals(
      decoded.entry(ChunkCoordinate.unsafe(Array(1L, 0L))),
      Right(entries(2))
    )

  test("shard index rejects checksum corruption and partial fill sentinels"):
    val shape = zvalue(Shape(1L))
    val index = zvalue(ShardIndex(shape, Vector(ShardIndexEntry.Fill)))
    val corrupted = zvalue(ShardIndexCodec.encode(index)).toArray
    corrupted(0) = 0
    assert(ShardIndexCodec.decode(OwnedBytes.copyOf(corrupted), shape).isLeft)

    val partial = zvalue(ShardIndexCodec.encode(index)).toArray
    partial(8) = 0
    val payload = OwnedBytes.copyOf(partial.dropRight(4))
    val corrected = Crc32c.append(payload)
    assert(ShardIndexCodec.decode(corrected, shape).isLeft)

  test("fixed-size index pipelines preserve exact raw length and round-trip"):
    val shape = zvalue(Shape(2L, 2L))
    val program = zvalue(
      ShardIndexProgram.compile(
        Vector(
          BytesCodec(Some(Endianness.Little)),
          ShuffleCodec(8),
          Crc32cCodec
        )
      )
    )
    assertEquals(program.encodedLength(shape, ShardIndexLimits()).map(_.toLong), Right(68L))
    val index = zvalue(
      ShardIndex(
        shape,
        Vector(
          ShardIndexEntry.Present(68L, count(8L)),
          ShardIndexEntry.Fill,
          ShardIndexEntry.Fill,
          ShardIndexEntry.Present(76L, count(8L))
        )
      )
    )
    val raw = zvalue(ShardIndexCodec.encodeRaw(index))
    val encoded = zvalue(
      SyncCodecRuntime.core.encodeBytes(
        raw,
        program.byteCodecs,
        count(128L)
      )
    )
    assertEquals(encoded.length, 68)
    val decodedRaw = zvalue(
      SyncCodecRuntime.core.decodeBytes(
        encoded,
        program.byteCodecs,
        Some(count(64L)),
        DecodeLimits.default
      )
    )
    assertEquals(zvalue(ShardIndexCodec.decodeRaw(decodedRaw, shape)).entries, index.entries)

  test("variable-size index codecs are rejected rather than guessed"):
    val metadata =
      """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4,4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"sharding_indexed","configuration":{"chunk_shape":[2,2],"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":1}}]}}],"attributes":{},"storage_transformers":[]}"""
    val parsed = zvalue(ZarrMetadata.parse(metadata)) match
      case ZarrNodeMetadata.Array(array) => ArrayDescriptor.compile(array)
      case _                             => Left(ZarrError.InvalidMetadata("$", "expected array"))
    assert(parsed.isLeft)

  test("Zarr-Python start-indexed shard is byte-compatible in both directions"):
    val innerGridShape = zvalue(Shape(2L, 2L))
    val encodedIndex = ZarrBinaryFixtures.shardedStartObject.slice(0, 68)
    val decoded = zvalue(ShardIndexCodec.decode(encodedIndex, innerGridShape))
    val expected = Vector(
      ShardIndexEntry.Present(68L, count(8L)),
      ShardIndexEntry.Fill,
      ShardIndexEntry.Fill,
      ShardIndexEntry.Present(76L, count(8L))
    )
    assertEquals(decoded.entries, expected)
    assertEquals(zvalue(ShardIndexCodec.encode(decoded)), encodedIndex)
    assertEquals(
      ZarrBinaryFixtures.shardedStartObject.slice(68, 76),
      ZarrBinaryFixtures.hex("0100020003000400")
    )
    assertEquals(
      ZarrBinaryFixtures.shardedStartObject.slice(76, 84),
      ZarrBinaryFixtures.hex("0d000e000f001000")
    )

  test("sharded grid requires exact inner division"):
    val arrayShape = zvalue(Shape(16L, 16L))
    val outer = zvalue(RegularGrid(arrayShape, zvalue(Shape(8L, 8L))))
    assert(ShardedGrid(outer, zvalue(Shape(3L, 4L))).isLeft)
    val valid = zvalue(ShardedGrid(outer, zvalue(Shape(4L, 4L))))
    assertEquals(valid.innerChunksPerShard, zvalue(Shape(2L, 2L)))
    assertEquals(valid.globalInnerGrid.gridShape, zvalue(Shape(4L, 4L)))

  test("arbitrary-rank inner demands group into deterministic shards"):
    val arrayShape = zvalue(Shape(16L, 16L))
    val outer = zvalue(RegularGrid(arrayShape, zvalue(Shape(8L, 8L))))
    val sharded = zvalue(ShardedGrid(outer, zvalue(Shape(4L, 4L))))
    val region = zvalue(
      Region.within(
        arrayShape,
        zvalue(Coordinate(2L, 2L)),
        zvalue(Shape(12L, 12L))
      )
    )
    val innerPlan = zvalue(ChunkPlanner.planRegion(sharded.globalInnerGrid, region))
    val plan = zvalue(ShardPlanner.group(sharded, innerPlan))
    assertEquals(plan.touchedInnerChunks, 16)
    assertEquals(
      plan.shards.map(_.coordinate.toVector),
      Vector(
        Vector(0L, 0L),
        Vector(0L, 1L),
        Vector(1L, 0L),
        Vector(1L, 1L)
      )
    )
    assert(plan.shards.forall(_.innerChunks.length == 4))

  test("start and end index plans make object-length requirements explicit"):
    val arrayShape = zvalue(Shape(16L, 16L))
    val outer = zvalue(RegularGrid(arrayShape, zvalue(Shape(8L, 8L))))
    val sharded = zvalue(ShardedGrid(outer, zvalue(Shape(4L, 4L))))
    val region = zvalue(Region.within(arrayShape, zvalue(Coordinate(0L, 0L)), arrayShape))
    val grouped = zvalue(
      ShardPlanner.group(
        sharded,
        zvalue(ChunkPlanner.planRegion(sharded.globalInnerGrid, region))
      )
    )
    val keys = DefaultChunkKeyEncoding(ChunkSeparator.Slash)
    val start = zvalue(
      ShardIndexReadPlan(
        grouped,
        sharded.innerChunksPerShard,
        IndexLocation.Start,
        keys
      )
    )
    assert(start.reads.forall(read => read.range.offset == 0L && read.range.length.toLong == 68L))
    assertEquals(start.indexBytes.toLong, 272L)

    assert(
      ShardIndexReadPlan(
        grouped,
        sharded.innerChunksPerShard,
        IndexLocation.End,
        keys
      ).isLeft
    )
    val lengths = grouped.shards.map(shard => shard.coordinate.toVector -> 200L).toMap
    val end = zvalue(
      ShardIndexReadPlan(
        grouped,
        sharded.innerChunksPerShard,
        IndexLocation.End,
        keys,
        lengths
      )
    )
    assert(end.reads.forall(_.range.offset == 132L))

  test("resolved shard plans retain fill chunks and bound range bytes"):
    val arrayShape = zvalue(Shape(8L, 8L))
    val outer = zvalue(RegularGrid(arrayShape, zvalue(Shape(8L, 8L))))
    val sharded = zvalue(ShardedGrid(outer, zvalue(Shape(4L, 4L))))
    val region = zvalue(Region.within(arrayShape, zvalue(Coordinate(0L, 0L)), arrayShape))
    val grouped = zvalue(
      ShardPlanner.group(
        sharded,
        zvalue(ChunkPlanner.planRegion(sharded.globalInnerGrid, region))
      )
    )
    val index = zvalue(
      ShardIndex(
        sharded.innerChunksPerShard,
        Vector(
          ShardIndexEntry.Present(68L, count(10L)),
          ShardIndexEntry.Fill,
          ShardIndexEntry.Present(78L, count(12L)),
          ShardIndexEntry.Present(90L, count(9L))
        )
      )
    )
    val resolved = zvalue(
      ShardDataPlan.resolve(
        grouped,
        Map(Vector(0L, 0L) -> index),
        DefaultChunkKeyEncoding(ChunkSeparator.Slash)
      )
    )
    assertEquals(resolved.rangeReads, 3)
    assertEquals(resolved.plannedBytes.toLong, 31L)
    assertEquals(
      resolved.shards.head.chunks.count(_.source == ShardChunkSource.Fill),
      1
    )

  test("rank-five sharding is not a special case"):
    val shape = zvalue(Shape(4L, 8L, 12L, 16L, 20L))
    val outer = zvalue(RegularGrid(shape, zvalue(Shape(2L, 4L, 6L, 8L, 10L))))
    val sharded = zvalue(ShardedGrid(outer, zvalue(Shape(1L, 2L, 3L, 4L, 5L))))
    val region = zvalue(
      Region.within(
        shape,
        zvalue(Coordinate(1L, 1L, 1L, 1L, 1L)),
        zvalue(Shape(2L, 3L, 4L, 5L, 6L))
      )
    )
    val grouped = zvalue(
      ShardPlanner.group(
        sharded,
        zvalue(ChunkPlanner.planRegion(sharded.globalInnerGrid, region))
      )
    )
    assertEquals(grouped.shards.map(_.coordinate.rank.toInt).distinct, Vector(5))
