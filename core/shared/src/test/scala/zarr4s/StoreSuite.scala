package zarr4s

class StoreSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def key(value: String): StoreKey = zvalue(StoreKey.from(value))

  private def range(offset: Long, length: Long): ByteRange = zvalue(ByteRange(offset, length))

  test("store keys reject traversal and encoded separators"):
    Seq("/absolute", "a/../b", "a//b", "a\\b", "a/%2f/b", "a/%5C/b").foreach: value =>
      assert(StoreKey.from(value).isLeft, value)
    assertEquals(StoreKey.from("canonical/c/0/1").map(_.value), Right("canonical/c/0/1"))

  test("memory store has exact range, whole, length, and trace semantics"):
    val store = zvalue(
      MemoryStore(
        Map(
          "a/b" -> OwnedBytes.copyOf(Array[Byte](0, 1, 2, 3, 4))
        )
      )
    )
    assertEquals(
      store.read(key("a/b"), range(1L, 3L)).map(_.toArray.toVector),
      Right(Vector[Byte](1, 2, 3))
    )
    assertEquals(store.length(key("a/b")), Right(5L))
    assertEquals(store.readAll(key("a/b"), zvalue(ByteCount(5L))).map(_.length), Right(5))
    assertEquals(
      store.trace,
      Vector(
        ObjectRequest.Range(key("a/b"), range(1L, 3L)),
        ObjectRequest.Length(key("a/b")),
        ObjectRequest.Whole(key("a/b"))
      )
    )

  test("memory store distinguishes missing, invalid range, and size limit"):
    val store = zvalue(
      MemoryStore(
        Map(
          "object" -> OwnedBytes.copyOf(Array[Byte](0, 1, 2, 3, 4))
        )
      )
    )
    assert(store.readAll(key("missing"), zvalue(ByteCount(10L))).isLeft)
    assert(store.read(key("object"), range(4L, 2L)).isLeft)
    assert(store.readAll(key("object"), zvalue(ByteCount(4L))).isLeft)

  test("memory store lists recursive descendants under an explicit bound"):
    val store = zvalue(
      MemoryStore(
        Map(
          "group/zarr.json" -> OwnedBytes.copyOf(Array[Byte](1)),
          "group/child/zarr.json" -> OwnedBytes.copyOf(Array[Byte](2)),
          "other" -> OwnedBytes.copyOf(Array[Byte](3))
        )
      )
    )
    assertEquals(
      store.list(zvalue(ZarrPath("group")), 10).map(_.map(_.value)),
      Right(Vector("group/child/zarr.json", "group/zarr.json"))
    )
    assert(store.list(ZarrPath.root, 2).isLeft)

  test("range coalescing is deterministic and bounded"):
    val ranges = Vector(
      range(30L, 5L) -> "c",
      range(0L, 10L) -> "a",
      range(12L, 5L) -> "b",
      range(100L, 10L) -> "d"
    )
    val result = zvalue(
      RangeCoalescer.coalesce(
        ranges,
        CoalescingLimits(3L, zvalue(ByteCount(40L)))
      )
    )
    assertEquals(
      result.map(found => found.range.offset -> found.range.length.toLong),
      Vector(
        0L -> 17L,
        30L -> 5L,
        100L -> 10L
      )
    )
    assertEquals(
      result.head.members.map(member => member.value -> member.relativeOffset),
      Vector(
        "a" -> 0,
        "b" -> 12
      )
    )
