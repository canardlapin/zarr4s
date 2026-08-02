package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
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

  private final case class ExpandingSync(name: String, extra: Int) extends SyncByteCodecExecutor:
    private def expand(bytes: OwnedBytes): OwnedBytes =
      OwnedBytes.copyOf(bytes.toArray ++ Array.fill[Byte](extra)(0))

    def decode(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        expectedDecoded: ByteCount,
        limits: DecodeLimits
    ): Either[CodecError, OwnedBytes] = Right(encoded)

    override def decodeBounded(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        limits: DecodeLimits
    ): Either[CodecError, OwnedBytes] = Right(expand(encoded))

    def encode(
        codec: CompiledCodec,
        decoded: OwnedBytes
    ): Either[CodecError, OwnedBytes] = Right(expand(decoded))

  private final case class ExpandingAsync(name: String, extra: Int) extends AsyncByteCodecExecutor:
    private def expand(bytes: OwnedBytes): OwnedBytes =
      OwnedBytes.copyOf(bytes.toArray ++ Array.fill[Byte](extra)(0))

    def decode(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        expectedDecoded: ByteCount,
        limits: DecodeLimits
    )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
      Future.successful(Right(encoded))

    override def decodeBounded(
        codec: CompiledCodec,
        encoded: OwnedBytes,
        limits: DecodeLimits
    )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
      Future.successful(Right(expand(encoded)))

    def encode(
        codec: CompiledCodec,
        decoded: OwnedBytes
    )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
      Future.successful(Right(expand(decoded)))

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def count(value: Long): ByteCount = ByteCount(value) match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytesProgram(name: String): CodecProgram =
    zvalue(
      CodecProgram.compile(
        CodecRepresentation.Bytes,
        Vector(TestByteCodec(name))
      )
    )

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

  test("sync bytes pipelines use exact decode lengths and enforce stage limits"):
    val runtime = zvalue(SyncCodecRuntime("expanding", Vector(ExpandingSync("expand", 4))))
    val program = bytesProgram("expand")
    val input = OwnedBytes.copyOf(Array[Byte](1, 2))
    assertEquals(
      runtime.decodeBytes(input, program, Some(count(2L)), DecodeLimits.default),
      Right(input)
    )
    assertEquals(
      runtime.decodeBytes(input, program, None, DecodeLimits(count(5L))),
      Left(
        ZarrError.CodecFailure(
          CodecError.DecodedLimitExceeded(5L, 6L)
        )
      )
    )
    assertEquals(
      runtime.encodeBytes(input, program, count(5L)),
      Left(ZarrError.ResourceLimit("encoded bytes", 5L, 6L))
    )

  test("async bytes pipelines use bounded decode and enforce encoded limits"):
    val runtime = zvalue(AsyncCodecRuntime("expanding", Vector(ExpandingAsync("expand", 4))))
    val program = bytesProgram("expand")
    val input = OwnedBytes.copyOf(Array[Byte](1, 2))
    runtime
      .decodeBytes(input, program, Some(count(2L)), DecodeLimits.default)
      .flatMap: exact =>
        assertEquals(exact, Right(input))
        runtime
          .decodeBytes(input, program, None, DecodeLimits(count(5L)))
          .flatMap: bounded =>
            assertEquals(
              bounded,
              Left(
                ZarrError.CodecFailure(
                  CodecError.DecodedLimitExceeded(5L, 6L)
                )
              )
            )
            runtime
              .encodeBytes(input, program, count(5L))
              .map: encoded =>
                assertEquals(encoded, Left(ZarrError.ResourceLimit("encoded bytes", 5L, 6L)))
