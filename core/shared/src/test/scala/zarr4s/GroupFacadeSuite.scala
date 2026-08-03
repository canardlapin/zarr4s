package zarr4s

import scala.concurrent.ExecutionContext

class GroupFacadeSuite extends munit.FunSuite:
  private given ExecutionContext = ExecutionContext.parasitic

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def json(fields: (String, JsonValue)*): JsonObject =
    JsonObject.from(fields).fold(fail(_), identity)

  private def complete(result: GroupWriteResult): WriteReceipt = result.outcome match
    case WriteOutcome.Complete(receipt)    => receipt
    case WriteOutcome.Incomplete(_, error) => fail(error.message)

  test("sync group facade preserves attributes, format, and create-only conflicts"):
    val store = value(MemoryStore.empty)
    val path = value(ZarrPath("study"))
    val attributes = json("title" -> JsonValue.Str("measurements"))
    val spec = GroupSpec(attributes)
    val first = SyncZarr.createGroup(store, spec, path)
    val receipt = complete(first)

    assertEquals(first.spec, spec)
    assertEquals(receipt.metadata.key.value, "study/zarr.json")
    assertEquals(value(SyncZarr.openGroup(store, path)).metadata.attributes, attributes)

    SyncZarr.createGroup(store, spec, path).outcome match
      case WriteOutcome.Incomplete(
            progress,
            ZarrError.StoreFailure(StoreError.AlreadyExists(_))
          ) =>
        assertEquals(progress.createdObjects, 0)
      case other => fail(s"expected a create-only conflict, found $other")

  test("sync group facade publishes v2 metadata through the same specification"):
    val store = value(MemoryStore.empty)
    val attributes = json("format" -> JsonValue.Str("v2"))
    val result = SyncZarr.createGroup(
      store,
      GroupSpec(attributes).asFormat(ZarrFormat.V2)
    )

    assertEquals(complete(result).metadata.key.value, ".zgroup")
    assertEquals(store.writeTrace.map(_.key.value), Vector(".zattrs", ".zgroup"))
    assertEquals(value(SyncZarr.openGroup(store)).metadata.attributes, attributes)

  test("async group facade creates and reopens a nested group"):
    val store = value(AsyncMemoryStore(Map.empty))
    val path = value(ZarrPath("experiment/session"))
    val attributes = json("subject" -> JsonValue.Str("s01"))

    AsyncZarr
      .createGroup(store, GroupSpec(attributes), path)
      .flatMap: result =>
        complete(result)
        AsyncZarr
          .openGroup(store, path, lister = Some(store))
          .map: opened =>
            assertEquals(value(opened).metadata.attributes, attributes)
