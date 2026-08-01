package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class CodecRuntimeSuite extends munit.FunSuite:
  private final case class TestByteCodec(name: String) extends CompiledCodec:
    val input = CodecRepresentation.Bytes
    val output = CodecRepresentation.Bytes
    val configuration = JsonObject.empty

  private final case class SyncIdentity(name: String) extends SyncByteCodecExecutor:
    def decode(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        expectedDecoded: ByteCount,
        limits: DecodeLimits
    ): Either[CodecError, OwnedBytes] = Right(encoded)

    def encode(
        codec: CompiledCodec,
        decoded: OwnedBytes
    ): Either[CodecError, OwnedBytes] = Right(decoded)

  private final case class AsyncIdentity(name: String) extends AsyncByteCodecExecutor:
    def decode(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        expectedDecoded: ByteCount,
        limits: DecodeLimits
    )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
      Future.successful(Right(encoded))

    def encode(
        codec: CompiledCodec,
        decoded: OwnedBytes
    )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
      Future.successful(Right(decoded))

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private val gzipProgram = zvalue(
    CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(
        BytesCodec(Some(Endianness.Little)),
        TestByteCodec("gzip")
      )
    )
  )

  test("runtime construction rejects ambiguous executor registries"):
    assertEquals(
      SyncCodecRuntime("test", Vector(SyncIdentity("xor"), SyncIdentity("xor"))),
      Left(ZarrError.InvalidCodecRuntime("duplicate executor 'xor' on test"))
    )
    assertEquals(
      AsyncCodecRuntime("test", Vector(AsyncIdentity("xor"), AsyncIdentity("xor"))),
      Left(ZarrError.InvalidCodecRuntime("duplicate executor 'xor' on test"))
    )

  test("metadata capability and executable capability remain separate"):
    assertEquals(
      SyncCodecRuntime.core.validate(gzipProgram),
      Left(
        ZarrError.CodecFailure(
          CodecError.UnsupportedCapability("gzip", "synchronous runtime")
        )
      )
    )
    assertEquals(
      AsyncCodecRuntime.core.validate(gzipProgram),
      Left(
        ZarrError.CodecFailure(
          CodecError.UnsupportedCapability("gzip", "asynchronous runtime")
        )
      )
    )

  test("runtime requirements are derived from the compiled program"):
    val runtime = zvalue(SyncCodecRuntime("test", Vector(SyncIdentity("gzip"))))
    assertEquals(runtime.executorNames, Vector("crc32c", "gzip", "shuffle"))
    assertEquals(runtime.validate(gzipProgram), Right(()))
