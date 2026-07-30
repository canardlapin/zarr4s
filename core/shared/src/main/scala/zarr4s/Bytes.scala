package zarr4s

opaque type ByteCount = Long

object ByteCount:
  def apply(value: Long): Either[ZarrError, ByteCount] =
    if value >= 0L then Right(value)
    else Left(ZarrError.InvalidSelection(s"byte count must be non-negative, found $value"))

  val zero: ByteCount = 0L

  private[zarr4s] def unsafe(value: Long): ByteCount = value

  extension (count: ByteCount) inline def toLong: Long = count

final class OwnedBytes private (private[zarr4s] val values: Array[Byte]):
  val length: Int = values.length
  val byteCount: ByteCount = ByteCount.unsafe(values.length.toLong)

  def apply(index: Int): Byte = values(index)

  def toArray: Array[Byte] = java.util.Arrays.copyOf(values, values.length)

  def slice(from: Int, until: Int): OwnedBytes =
    require(from >= 0 && until >= from && until <= values.length, "invalid byte slice")
    OwnedBytes.unsafe(java.util.Arrays.copyOfRange(values, from, until))

  override def equals(other: Any): Boolean = other match
    case that: OwnedBytes => java.util.Arrays.equals(values, that.values)
    case _                => false

  override def hashCode(): Int = java.util.Arrays.hashCode(values)

object OwnedBytes:
  val empty: OwnedBytes = new OwnedBytes(Array.emptyByteArray)

  def copyOf(values: Array[Byte]): OwnedBytes =
    new OwnedBytes(java.util.Arrays.copyOf(values, values.length))

  private[zarr4s] def unsafe(values: Array[Byte]): OwnedBytes = new OwnedBytes(values)

final case class ByteRange private (offset: Long, length: ByteCount):
  def endExclusive: Either[ZarrError, Long] =
    LongArrays.checkedAdd(offset, length.toLong, "byte range end")

object ByteRange:
  def apply(offset: Long, length: Long): Either[ZarrError, ByteRange] =
    if offset < 0L then
      Left(ZarrError.InvalidSelection(s"byte offset must be non-negative, found $offset"))
    else
      ByteCount(length).flatMap: count =>
        LongArrays
          .checkedAdd(offset, length, "byte range end")
          .map(_ => new ByteRange(offset, count))

enum CodecError:
  case UnsupportedCapability(codec: String, platform: String)
  case UnsupportedDataType(name: String)
  case InvalidBlockType(dataType: String)
  case InvalidEncodedLength(expected: Long, actual: Long)
  case InvalidDecodedLength(expected: Long, actual: Long)
  case DecodedLimitExceeded(limit: Long, requested: Long)
  case ChecksumMismatch(expected: Long, actual: Long)
  case CorruptData(codec: String, detail: String)

  def message: String = this match
    case UnsupportedCapability(codec, platform) => s"$codec is unavailable on $platform"
    case UnsupportedDataType(name)              => s"unsupported scalar byte data type '$name'"
    case InvalidBlockType(dataType) => s"primitive block does not match data type '$dataType'"
    case InvalidEncodedLength(expected, actual) =>
      s"encoded length mismatch: expected $expected bytes, found $actual"
    case InvalidDecodedLength(expected, actual) =>
      s"decoded length mismatch: expected $expected bytes, found $actual"
    case DecodedLimitExceeded(limit, requested) =>
      s"decoded byte limit exceeded: limit $limit, requested $requested"
    case ChecksumMismatch(expected, actual) =>
      s"CRC32C mismatch: expected 0x${hex(expected)}, found 0x${hex(actual)}"
    case CorruptData(codec, detail) => s"corrupt $codec data: $detail"

  private def hex(value: Long): String =
    java.lang.Long.toHexString(value & 0xffffffffL)

final case class DecodeLimits(maxDecodedBytes: ByteCount)

object DecodeLimits:
  val default: DecodeLimits = DecodeLimits(ByteCount.unsafe(512L * 1024L * 1024L))

object DecodedLength:
  def validate(
      bytes: OwnedBytes,
      expected: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes] =
    if expected.toLong > limits.maxDecodedBytes.toLong then
      Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, expected.toLong))
    else if bytes.byteCount != expected then
      Left(CodecError.InvalidDecodedLength(expected.toLong, bytes.byteCount.toLong))
    else Right(bytes)

object Crc32c:
  private val table: Array[Int] =
    val result = new Array[Int](256)
    var index = 0
    while index < result.length do
      var crc = index
      var bit = 0
      while bit < 8 do
        crc = if (crc & 1) != 0 then (crc >>> 1) ^ 0x82f63b78 else crc >>> 1
        bit += 1
      result(index) = crc
      index += 1
    result

  def checksum(bytes: OwnedBytes): Long = checksum(bytes.values, 0, bytes.length)

  private[zarr4s] def checksum(bytes: Array[Byte], offset: Int, length: Int): Long =
    var crc = 0xffffffff
    var index = offset
    val end = offset + length
    while index < end do
      crc = table((crc ^ (bytes(index) & 0xff)) & 0xff) ^ (crc >>> 8)
      index += 1
    (~crc).toLong & 0xffffffffL

  def append(bytes: OwnedBytes): OwnedBytes =
    val result = new Array[Byte](bytes.length + 4)
    Array.copy(bytes.values, 0, result, 0, bytes.length)
    putLittleEndianUInt32(result, bytes.length, checksum(bytes))
    OwnedBytes.unsafe(result)

  def verifyAndStrip(bytes: OwnedBytes): Either[CodecError, OwnedBytes] =
    if bytes.length < 4 then
      Left(CodecError.CorruptData("crc32c", "payload is shorter than checksum"))
    else
      val payloadLength = bytes.length - 4
      val expected = getLittleEndianUInt32(bytes.values, payloadLength)
      val actual = checksum(bytes.values, 0, payloadLength)
      if expected != actual then Left(CodecError.ChecksumMismatch(expected, actual))
      else Right(bytes.slice(0, payloadLength))

  private[zarr4s] def putLittleEndianUInt32(bytes: Array[Byte], offset: Int, value: Long): Unit =
    var shift = 0
    while shift < 32 do
      bytes(offset + shift / 8) = ((value >>> shift) & 0xffL).toByte
      shift += 8

  private[zarr4s] def getLittleEndianUInt32(bytes: Array[Byte], offset: Int): Long =
    var result = 0L
    var index = 0
    while index < 4 do
      result |= (bytes(offset + index).toLong & 0xffL) << (index * 8)
      index += 1
    result
