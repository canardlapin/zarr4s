package zarr4s

final class OwnedBooleans private (private[zarr4s] val values: Array[Boolean]):
  val length: Int = values.length
  def apply(index: Int): Boolean = values(index)
  def toArray: Array[Boolean] = java.util.Arrays.copyOf(values, values.length)

object OwnedBooleans:
  def copyOf(values: Array[Boolean]): OwnedBooleans =
    new OwnedBooleans(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Boolean]): OwnedBooleans = new OwnedBooleans(values)

final class OwnedShorts private (private[zarr4s] val values: Array[Short]):
  val length: Int = values.length
  def apply(index: Int): Short = values(index)
  def toArray: Array[Short] = java.util.Arrays.copyOf(values, values.length)

object OwnedShorts:
  def copyOf(values: Array[Short]): OwnedShorts =
    new OwnedShorts(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Short]): OwnedShorts = new OwnedShorts(values)

final class OwnedInts private (private[zarr4s] val values: Array[Int]):
  val length: Int = values.length
  def apply(index: Int): Int = values(index)
  def toArray: Array[Int] = java.util.Arrays.copyOf(values, values.length)

object OwnedInts:
  def copyOf(values: Array[Int]): OwnedInts =
    new OwnedInts(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Int]): OwnedInts = new OwnedInts(values)

final class OwnedLongs private (private[zarr4s] val values: Array[Long]):
  val length: Int = values.length
  def apply(index: Int): Long = values(index)
  def toArray: Array[Long] = java.util.Arrays.copyOf(values, values.length)

object OwnedLongs:
  def copyOf(values: Array[Long]): OwnedLongs =
    new OwnedLongs(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Long]): OwnedLongs = new OwnedLongs(values)

final class OwnedFloats private (private[zarr4s] val values: Array[Float]):
  val length: Int = values.length
  def apply(index: Int): Float = values(index)
  def toArray: Array[Float] = java.util.Arrays.copyOf(values, values.length)

object OwnedFloats:
  def copyOf(values: Array[Float]): OwnedFloats =
    new OwnedFloats(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Float]): OwnedFloats = new OwnedFloats(values)

final class OwnedDoubles private (private[zarr4s] val values: Array[Double]):
  val length: Int = values.length
  def apply(index: Int): Double = values(index)
  def toArray: Array[Double] = java.util.Arrays.copyOf(values, values.length)

object OwnedDoubles:
  def copyOf(values: Array[Double]): OwnedDoubles =
    new OwnedDoubles(java.util.Arrays.copyOf(values, values.length))
  private[zarr4s] def unsafe(values: Array[Double]): OwnedDoubles = new OwnedDoubles(values)

/** Interleaved real/imaginary primitive storage for complex64 values. */
final class OwnedComplex64 private (private[zarr4s] val values: Array[Float]):
  val length: Int = values.length / 2

  def real(index: Int): Float = values(index * 2)
  def imaginary(index: Int): Float = values(index * 2 + 1)
  def toInterleavedArray: Array[Float] = java.util.Arrays.copyOf(values, values.length)

object OwnedComplex64:
  def fromInterleaved(values: Array[Float]): Either[ZarrError, OwnedComplex64] =
    if values.length % 2 != 0 then
      Left(ZarrError.InvalidSelection("complex64 storage must contain pairs of components"))
    else Right(copyOfInterleaved(values))

  private[zarr4s] def copyOfInterleaved(values: Array[Float]): OwnedComplex64 =
    new OwnedComplex64(java.util.Arrays.copyOf(values, values.length))

  private[zarr4s] def unsafe(values: Array[Float]): OwnedComplex64 = new OwnedComplex64(values)

  private[zarr4s] def filledBits(size: Int, realBits: Int, imaginaryBits: Int): OwnedComplex64 =
    val values = new Array[Float](size * 2)
    val real = java.lang.Float.intBitsToFloat(realBits)
    val imaginary = java.lang.Float.intBitsToFloat(imaginaryBits)
    var index = 0
    while index < size do
      values(index * 2) = real
      values(index * 2 + 1) = imaginary
      index += 1
    new OwnedComplex64(values)

/** Interleaved real/imaginary primitive storage for complex128 values. */
final class OwnedComplex128 private (private[zarr4s] val values: Array[Double]):
  val length: Int = values.length / 2

  def real(index: Int): Double = values(index * 2)
  def imaginary(index: Int): Double = values(index * 2 + 1)
  def toInterleavedArray: Array[Double] = java.util.Arrays.copyOf(values, values.length)

object OwnedComplex128:
  def fromInterleaved(values: Array[Double]): Either[ZarrError, OwnedComplex128] =
    if values.length % 2 != 0 then
      Left(ZarrError.InvalidSelection("complex128 storage must contain pairs of components"))
    else Right(copyOfInterleaved(values))

  private[zarr4s] def copyOfInterleaved(values: Array[Double]): OwnedComplex128 =
    new OwnedComplex128(java.util.Arrays.copyOf(values, values.length))

  private[zarr4s] def unsafe(values: Array[Double]): OwnedComplex128 = new OwnedComplex128(values)

  private[zarr4s] def filledBits(size: Int, realBits: Long, imaginaryBits: Long): OwnedComplex128 =
    val values = new Array[Double](size * 2)
    val real = java.lang.Double.longBitsToDouble(realBits)
    val imaginary = java.lang.Double.longBitsToDouble(imaginaryBits)
    var index = 0
    while index < size do
      values(index * 2) = real
      values(index * 2 + 1) = imaginary
      index += 1
    new OwnedComplex128(values)

/** Exact IEEE 754 binary16 conversion shared by JVM and Scala.js. */
object HalfFloat:
  def toBits(value: Float): Int =
    val bits = java.lang.Float.floatToRawIntBits(value)
    val sign = (bits >>> 16) & 0x8000
    val exponent = (bits >>> 23) & 0xff
    val mantissa = bits & 0x7fffff
    if exponent == 0xff then
      if mantissa == 0 then sign | 0x7c00
      else sign | 0x7e00 | ((mantissa >>> 13) & 0x03ff)
    else
      val halfExponent = exponent - 127 + 15
      if halfExponent >= 0x1f then sign | 0x7c00
      else if halfExponent <= 0 then
        if halfExponent < -10 then sign
        else
          val normalized = mantissa | 0x800000
          val shift = 14 - halfExponent
          var result = normalized >>> shift
          val remainder = normalized & ((1 << shift) - 1)
          val halfway = 1 << (shift - 1)
          if remainder > halfway || (remainder == halfway && (result & 1) != 0) then result += 1
          sign | result
      else
        var result = (halfExponent << 10) | (mantissa >>> 13)
        val remainder = mantissa & 0x1fff
        if remainder > 0x1000 || (remainder == 0x1000 && (result & 1) != 0) then
          result += 1
          if (result & 0x7c00) == 0x7c00 then result = sign | 0x7c00
        sign | (result & 0x7fff)

  def fromBits(bits: Int): Float =
    val sign = (bits & 0x8000) << 16
    val exponent = (bits >>> 10) & 0x1f
    val mantissa = bits & 0x03ff
    val result =
      if exponent == 0 then
        if mantissa == 0 then sign
        else
          var normalized = mantissa
          var exponentValue = -14
          while (normalized & 0x0400) == 0 do
            normalized <<= 1
            exponentValue -= 1
          sign | ((exponentValue + 127) << 23) | ((normalized & 0x03ff) << 13)
      else if exponent == 0x1f then sign | 0x7f800000 | (mantissa << 13)
      else sign | ((exponent - 15 + 127) << 23) | (mantissa << 13)
    java.lang.Float.intBitsToFloat(result)

/** The primitive carrier used by an executable fixed-width data type.
  *
  * Signed and unsigned values deliberately share the same primitive arrays: unsigned interpretation
  * belongs to the data type, while the hot path keeps compact JVM/Scala.js primitive storage and
  * exact raw bits.
  */
enum ScalarKind(val byteWidth: Int):
  case Bool extends ScalarKind(1)
  case Signed8 extends ScalarKind(1)
  case Unsigned8 extends ScalarKind(1)
  case Signed16 extends ScalarKind(2)
  case Unsigned16 extends ScalarKind(2)
  case Signed32 extends ScalarKind(4)
  case Unsigned32 extends ScalarKind(4)
  case Signed64 extends ScalarKind(8)
  case Unsigned64 extends ScalarKind(8)
  case Float32 extends ScalarKind(4)
  case Float64 extends ScalarKind(8)
  case Float16 extends ScalarKind(2)
  case Complex64 extends ScalarKind(8)
  case Complex128 extends ScalarKind(16)
  case Raw private[zarr4s] (width: Int) extends ScalarKind(width)

  def accepts(block: PrimitiveBlock): Boolean = (this, block) match
    case (Bool, PrimitiveBlock.Bool(_))             => true
    case (Signed8, PrimitiveBlock.Int8(_))          => true
    case (Unsigned8, PrimitiveBlock.UInt8(_))       => true
    case (Signed16, PrimitiveBlock.Int16(_))        => true
    case (Unsigned16, PrimitiveBlock.UInt16(_))     => true
    case (Signed32, PrimitiveBlock.Int32(_))        => true
    case (Unsigned32, PrimitiveBlock.UInt32(_))     => true
    case (Signed64, PrimitiveBlock.Int64(_))        => true
    case (Unsigned64, PrimitiveBlock.UInt64(_))     => true
    case (Float16, PrimitiveBlock.Float16(_))       => true
    case (Float32, PrimitiveBlock.Float32(_))       => true
    case (Float64, PrimitiveBlock.Float64(_))       => true
    case (Complex64, PrimitiveBlock.Complex64(_))   => true
    case (Complex128, PrimitiveBlock.Complex128(_)) => true
    case (Raw(width), PrimitiveBlock.Raw(values, elementWidth))
        if width > 0 && width == elementWidth && values.length % elementWidth == 0 =>
      true
    case _ => false

  private[zarr4s] def allocate(
      fill: StoredScalar,
      size: Int,
      dataTypeName: String
  ): Either[ZarrError, PrimitiveBlock] = this match
    case Bool =>
      ScalarFill
        .boolean(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.Bool(OwnedBooleans.unsafe(Array.fill(size)(value)))
    case Signed8 =>
      ScalarFill
        .signed(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.Int8(OwnedBytes.unsafe(Array.fill(size)(value.toByte)))
    case Unsigned8 =>
      ScalarFill
        .unsigned(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.UInt8(OwnedBytes.unsafe(Array.fill(size)(value.toByte)))
    case Signed16 =>
      ScalarFill
        .signed(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.Int16(OwnedShorts.unsafe(Array.fill(size)(value.toShort)))
    case Unsigned16 =>
      ScalarFill
        .unsigned(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.UInt16(OwnedShorts.unsafe(Array.fill(size)(value.toShort)))
    case Signed32 =>
      ScalarFill
        .signed(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.Int32(OwnedInts.unsafe(Array.fill(size)(value.toInt)))
    case Unsigned32 =>
      ScalarFill
        .unsigned(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.UInt32(OwnedInts.unsafe(Array.fill(size)(value.toInt)))
    case Signed64 =>
      ScalarFill
        .signed(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.Int64(OwnedLongs.unsafe(Array.fill(size)(value)))
    case Unsigned64 =>
      ScalarFill
        .unsigned(fill, dataTypeName)
        .map: value =>
          PrimitiveBlock.UInt64(OwnedLongs.unsafe(Array.fill(size)(value)))
    case Float32 =>
      ScalarFill
        .floating(fill, 8)
        .map: bits =>
          PrimitiveBlock.Float32(
            OwnedFloats.unsafe(Array.fill(size)(java.lang.Float.intBitsToFloat(bits.toInt)))
          )
    case Float64 =>
      ScalarFill
        .floating(fill, 16)
        .map: bits =>
          PrimitiveBlock.Float64(
            OwnedDoubles.unsafe(Array.fill(size)(java.lang.Double.longBitsToDouble(bits)))
          )
    case Float16 =>
      ScalarFill
        .floating(fill, 4)
        .map: bits =>
          PrimitiveBlock.Float16(OwnedShorts.unsafe(Array.fill(size)(bits.toShort)))
    case Complex64 =>
      if size > Int.MaxValue / 2 then
        Left(ZarrError.ResourceLimit("complex64 fill allocation", Int.MaxValue, size.toLong * 2L))
      else
        ScalarFill
          .complex(fill, 8, dataTypeName)
          .map: (realBits, imaginaryBits) =>
            PrimitiveBlock.Complex64(
              OwnedComplex64.filledBits(size, realBits.toInt, imaginaryBits.toInt)
            )
    case Complex128 =>
      if size > Int.MaxValue / 2 then
        Left(ZarrError.ResourceLimit("complex128 fill allocation", Int.MaxValue, size.toLong * 2L))
      else
        ScalarFill
          .complex(fill, 16, dataTypeName)
          .map: (realBits, imaginaryBits) =>
            PrimitiveBlock.Complex128(
              OwnedComplex128.filledBits(size, realBits, imaginaryBits)
            )
    case Raw(width) =>
      ScalarFill
        .raw(fill, width, dataTypeName)
        .flatMap: values =>
          if size != 0 && width > Int.MaxValue / size then
            Left(ZarrError.ResourceLimit("raw fill allocation", Int.MaxValue, size.toLong * width))
          else
            val bytes = new Array[Byte](size * width)
            var index = 0
            while index < size do
              Array.copy(values, 0, bytes, index * width, width)
              index += 1
            Right(PrimitiveBlock.Raw(OwnedBytes.unsafe(bytes), width))

object ScalarKind:
  def raw(width: Int): Either[ZarrError, ScalarKind] =
    if width > 0 then Right(Raw(width))
    else Left(ZarrError.InvalidSelection(s"raw element width must be positive, found $width"))

enum PrimitiveBlock:
  case Bool(values: OwnedBooleans)
  case Int8(values: OwnedBytes)
  case UInt8(values: OwnedBytes)
  case Int16(values: OwnedShorts)
  case UInt16(values: OwnedShorts)
  case Int32(values: OwnedInts)
  case UInt32(values: OwnedInts)
  case Int64(values: OwnedLongs)
  case UInt64(values: OwnedLongs)
  case Float16(values: OwnedShorts)
  case Float32(values: OwnedFloats)
  case Float64(values: OwnedDoubles)
  case Complex64(values: OwnedComplex64)
  case Complex128(values: OwnedComplex128)
  case Raw private[zarr4s] (values: OwnedBytes, elementWidth: Int)

  def elementCount: Int = this match
    case Bool(values)              => values.length
    case Int8(values)              => values.length
    case UInt8(values)             => values.length
    case Int16(values)             => values.length
    case UInt16(values)            => values.length
    case Int32(values)             => values.length
    case UInt32(values)            => values.length
    case Int64(values)             => values.length
    case UInt64(values)            => values.length
    case Float16(values)           => values.length
    case Float32(values)           => values.length
    case Float64(values)           => values.length
    case Complex64(values)         => values.length
    case Complex128(values)        => values.length
    case Raw(values, elementWidth) => values.length / elementWidth

  private[zarr4s] def copyElementFrom(
      source: PrimitiveBlock,
      sourceIndex: Int,
      destinationIndex: Int
  ): Either[ZarrError, Unit] = (this, source) match
    case (Bool(target), Bool(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Int8(target), Int8(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (UInt8(target), UInt8(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Int16(target), Int16(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (UInt16(target), UInt16(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Int32(target), Int32(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (UInt32(target), UInt32(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Int64(target), Int64(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (UInt64(target), UInt64(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Float16(target), Float16(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Float32(target), Float32(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Float64(target), Float64(found)) =>
      target.values(destinationIndex) = found(sourceIndex)
      Right(())
    case (Complex64(target), Complex64(found)) =>
      target.values(destinationIndex * 2) = found.values(sourceIndex * 2)
      target.values(destinationIndex * 2 + 1) = found.values(sourceIndex * 2 + 1)
      Right(())
    case (Complex128(target), Complex128(found)) =>
      target.values(destinationIndex * 2) = found.values(sourceIndex * 2)
      target.values(destinationIndex * 2 + 1) = found.values(sourceIndex * 2 + 1)
      Right(())
    case (Raw(target, targetWidth), Raw(found, sourceWidth)) if targetWidth == sourceWidth =>
      Array.copy(
        found.values,
        sourceIndex * sourceWidth,
        target.values,
        destinationIndex * targetWidth,
        targetWidth
      )
      Right(())
    case _ => Left(ZarrError.InvalidSelection("primitive block dtype mismatch"))

  private[zarr4s] def reordered(sourceIndices: Array[Int]): PrimitiveBlock = this match
    case Bool(values)      => Bool(OwnedBooleans.unsafe(reorder(values.values, sourceIndices)))
    case Int8(values)      => Int8(OwnedBytes.unsafe(reorder(values.values, sourceIndices)))
    case UInt8(values)     => UInt8(OwnedBytes.unsafe(reorder(values.values, sourceIndices)))
    case Int16(values)     => Int16(OwnedShorts.unsafe(reorder(values.values, sourceIndices)))
    case UInt16(values)    => UInt16(OwnedShorts.unsafe(reorder(values.values, sourceIndices)))
    case Int32(values)     => Int32(OwnedInts.unsafe(reorder(values.values, sourceIndices)))
    case UInt32(values)    => UInt32(OwnedInts.unsafe(reorder(values.values, sourceIndices)))
    case Int64(values)     => Int64(OwnedLongs.unsafe(reorder(values.values, sourceIndices)))
    case UInt64(values)    => UInt64(OwnedLongs.unsafe(reorder(values.values, sourceIndices)))
    case Float16(values)   => Float16(OwnedShorts.unsafe(reorder(values.values, sourceIndices)))
    case Float32(values)   => Float32(OwnedFloats.unsafe(reorder(values.values, sourceIndices)))
    case Float64(values)   => Float64(OwnedDoubles.unsafe(reorder(values.values, sourceIndices)))
    case Complex64(values) =>
      Complex64(OwnedComplex64.unsafe(reorderPairs(values.values, sourceIndices)))
    case Complex128(values) =>
      Complex128(OwnedComplex128.unsafe(reorderPairs(values.values, sourceIndices)))
    case Raw(values, elementWidth) =>
      Raw(OwnedBytes.unsafe(reorderBytes(values.values, elementWidth, sourceIndices)), elementWidth)

  private def reorder[A: reflect.ClassTag](values: Array[A], sourceIndices: Array[Int]): Array[A] =
    val result = new Array[A](sourceIndices.length)
    var index = 0
    while index < result.length do
      result(index) = values(sourceIndices(index))
      index += 1
    result

  private def reorderPairs[A: reflect.ClassTag](
      values: Array[A],
      sourceIndices: Array[Int]
  ): Array[A] =
    val result = new Array[A](sourceIndices.length * 2)
    var index = 0
    while index < sourceIndices.length do
      val source = sourceIndices(index) * 2
      result(index * 2) = values(source)
      result(index * 2 + 1) = values(source + 1)
      index += 1
    result

  private def reorderBytes(
      values: Array[Byte],
      elementWidth: Int,
      sourceIndices: Array[Int]
  ): Array[Byte] =
    val result = new Array[Byte](sourceIndices.length * elementWidth)
    var index = 0
    while index < sourceIndices.length do
      Array.copy(
        values,
        sourceIndices(index) * elementWidth,
        result,
        index * elementWidth,
        elementWidth
      )
      index += 1
    result

object PrimitiveBlock:
  def raw(values: OwnedBytes, elementWidth: Int): Either[ZarrError, PrimitiveBlock] =
    if elementWidth <= 0 then
      Left(ZarrError.InvalidSelection(s"raw element width must be positive, found $elementWidth"))
    else if values.length % elementWidth != 0 then
      Left(
        ZarrError.InvalidSelection(
          s"raw byte length ${values.length} is not divisible by element width $elementWidth"
        )
      )
    else Right(PrimitiveBlock.Raw(values, elementWidth))

private object ScalarFill:
  def boolean(fill: StoredScalar, name: String): Either[ZarrError, Boolean] = fill match
    case StoredScalar.Boolean(value) => Right(value)
    case _ => Left(ZarrError.InvalidMetadata("$.fill_value", s"$name requires a boolean fill"))

  def signed(fill: StoredScalar, name: String): Either[ZarrError, Long] = fill match
    case StoredScalar.Integral(value) => Right(value)
    case _                            =>
      Left(ZarrError.InvalidMetadata("$.fill_value", s"$name requires a signed integral fill"))

  def unsigned(fill: StoredScalar, name: String): Either[ZarrError, Long] = fill match
    case StoredScalar.UnsignedIntegral(value)        => Right(value.toLong)
    case StoredScalar.Integral(value) if value >= 0L => Right(value)
    case _                                           =>
      Left(ZarrError.InvalidMetadata("$.fill_value", s"$name requires an unsigned integral fill"))

  def floating(fill: StoredScalar, hexDigits: Int): Either[ZarrError, Long] = fill match
    case StoredScalar.Floating(value) =>
      if hexDigits == 4 then Right(HalfFloat.toBits(value.toFloat).toLong)
      else if hexDigits == 8 then Right(java.lang.Float.floatToRawIntBits(value.toFloat).toLong)
      else Right(java.lang.Double.doubleToRawLongBits(value))
    case StoredScalar.FloatingBits(hex) => Right(parseHexBits(hex.drop(2)))
    case _                              =>
      Left(ZarrError.InvalidMetadata("$.fill_value", "floating dtype requires floating fill"))

  def complex(
      fill: StoredScalar,
      hexDigits: Int,
      name: String
  ): Either[ZarrError, (Long, Long)] = fill match
    case StoredScalar.Complex(real, imaginary) =>
      for
        realBits <- floatingComponent(real, hexDigits, name)
        imaginaryBits <- floatingComponent(imaginary, hexDigits, name)
      yield (realBits, imaginaryBits)
    case _ =>
      Left(ZarrError.InvalidMetadata("$.fill_value", s"$name requires a two-component fill"))

  def raw(fill: StoredScalar, width: Int, name: String): Either[ZarrError, Array[Byte]] = fill match
    case StoredScalar.RawBytes(values) =>
      if values.length != width then
        Left(
          ZarrError.InvalidMetadata(
            "$.fill_value",
            s"$name requires exactly $width raw bytes, found ${values.length}"
          )
        )
      else
        val result = new Array[Byte](values.length)
        var index = 0
        while index < values.length do
          val value = values(index)
          if value < 0 || value > 255 then
            return Left(
              ZarrError.InvalidMetadata(
                "$.fill_value",
                s"$name fill byte $value is outside the range [0, 255]"
              )
            )
          result(index) = value.toByte
          index += 1
        Right(result)
    case _ => Left(ZarrError.InvalidMetadata("$.fill_value", s"$name requires a byte array fill"))

  private def floatingComponent(
      value: StoredFloating,
      hexDigits: Int,
      name: String
  ): Either[ZarrError, Long] = value match
    case StoredFloating.Value(found) =>
      if hexDigits == 8 then Right(java.lang.Float.floatToRawIntBits(found.toFloat).toLong)
      else Right(java.lang.Double.doubleToRawLongBits(found))
    case StoredFloating.Bits(hex) =>
      if hex.length == hexDigits + 2 && hex.startsWith("0x") then Right(parseHexBits(hex.drop(2)))
      else
        Left(ZarrError.InvalidMetadata("$.fill_value", s"invalid $name floating component '$hex'"))

  private def parseHexBits(hex: String): Long =
    var result = 0L
    var index = 0
    while index < hex.length do
      result = (result << 4) | Character.digit(hex.charAt(index), 16).toLong
      index += 1
    result

object ScalarBytes:
  def decode(
      encoded: OwnedBytes,
      dataType: DataTypeCapability,
      endianness: Option[Endianness],
      elementCount: Long,
      limits: DecodeLimits = DecodeLimits.default
  ): Either[CodecError, PrimitiveBlock] =
    expectedBytes(dataType, elementCount, limits).flatMap: expected =>
      DecodedLength
        .validate(encoded, expected, limits)
        .flatMap: bytes =>
          if elementCount > Int.MaxValue.toLong then
            Left(CodecError.DecodedLimitExceeded(Int.MaxValue, elementCount))
          else
            val count = elementCount.toInt
            val order = byteOrder(endianness)
            dataType.scalarKind match
              case ScalarKind.Bool    => decodeBooleans(bytes.values).map(PrimitiveBlock.Bool.apply)
              case ScalarKind.Signed8 => Right(PrimitiveBlock.Int8(OwnedBytes.copyOf(bytes.values)))
              case ScalarKind.Unsigned8 =>
                Right(PrimitiveBlock.UInt8(OwnedBytes.copyOf(bytes.values)))
              case ScalarKind.Signed16 =>
                Right(PrimitiveBlock.Int16(decodeShorts(bytes.values, count, order)))
              case ScalarKind.Unsigned16 =>
                Right(PrimitiveBlock.UInt16(decodeShorts(bytes.values, count, order)))
              case ScalarKind.Signed32 =>
                Right(PrimitiveBlock.Int32(decodeInts(bytes.values, count, order)))
              case ScalarKind.Unsigned32 =>
                Right(PrimitiveBlock.UInt32(decodeInts(bytes.values, count, order)))
              case ScalarKind.Signed64 =>
                Right(PrimitiveBlock.Int64(decodeLongs(bytes.values, count, order)))
              case ScalarKind.Unsigned64 =>
                Right(PrimitiveBlock.UInt64(decodeLongs(bytes.values, count, order)))
              case ScalarKind.Float16 =>
                Right(PrimitiveBlock.Float16(decodeShorts(bytes.values, count, order)))
              case ScalarKind.Float32 =>
                Right(PrimitiveBlock.Float32(decodeFloats(bytes.values, count, order)))
              case ScalarKind.Float64 =>
                Right(PrimitiveBlock.Float64(decodeDoubles(bytes.values, count, order)))
              case ScalarKind.Complex64 =>
                componentCount(count, 2).map: components =>
                  PrimitiveBlock.Complex64(decodeComplex64(bytes.values, components, order))
              case ScalarKind.Complex128 =>
                componentCount(count, 2).map: components =>
                  PrimitiveBlock.Complex128(decodeComplex128(bytes.values, components, order))
              case ScalarKind.Raw(width) =>
                Right(PrimitiveBlock.Raw(OwnedBytes.copyOf(bytes.values), width))

  def encode(
      block: PrimitiveBlock,
      dataType: DataTypeCapability,
      endianness: Option[Endianness]
  ): Either[CodecError, OwnedBytes] =
    if !dataType.scalarKind.accepts(block) then Left(CodecError.InvalidBlockType(dataType.name))
    else
      val order = byteOrder(endianness)
      block match
        case PrimitiveBlock.Bool(values)       => Right(encodeBooleans(values.values))
        case PrimitiveBlock.Int8(values)       => Right(OwnedBytes.copyOf(values.values))
        case PrimitiveBlock.UInt8(values)      => Right(OwnedBytes.copyOf(values.values))
        case PrimitiveBlock.Int16(values)      => Right(encodeShorts(values.values, order))
        case PrimitiveBlock.UInt16(values)     => Right(encodeShorts(values.values, order))
        case PrimitiveBlock.Int32(values)      => Right(encodeInts(values.values, order))
        case PrimitiveBlock.UInt32(values)     => Right(encodeInts(values.values, order))
        case PrimitiveBlock.Int64(values)      => Right(encodeLongs(values.values, order))
        case PrimitiveBlock.UInt64(values)     => Right(encodeLongs(values.values, order))
        case PrimitiveBlock.Float16(values)    => Right(encodeShorts(values.values, order))
        case PrimitiveBlock.Float32(values)    => Right(encodeFloats(values.values, order))
        case PrimitiveBlock.Float64(values)    => Right(encodeDoubles(values.values, order))
        case PrimitiveBlock.Complex64(values)  => Right(encodeFloats(values.values, order))
        case PrimitiveBlock.Complex128(values) => Right(encodeDoubles(values.values, order))
        case PrimitiveBlock.Raw(values, _)     => Right(OwnedBytes.copyOf(values.values))

  private def expectedBytes(
      dataType: DataTypeCapability,
      elementCount: Long,
      limits: DecodeLimits
  ): Either[CodecError, ByteCount] =
    if elementCount < 0L then Left(CodecError.CorruptData("bytes", "negative element count"))
    else if elementCount != 0L && elementCount > Long.MaxValue / dataType.byteWidth.toLong then
      Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, Long.MaxValue))
    else
      val requested = elementCount * dataType.byteWidth.toLong
      if requested > limits.maxDecodedBytes.toLong then
        Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, requested))
      else Right(ByteCount.unsafe(requested))

  private def byteOrder(endianness: Option[Endianness]): Endianness =
    endianness.getOrElse(Endianness.Little)

  private def componentCount(count: Int, multiplier: Int): Either[CodecError, Int] =
    if count > Int.MaxValue / multiplier then
      Left(CodecError.DecodedLimitExceeded(Int.MaxValue, count.toLong * multiplier))
    else Right(count * multiplier)

  private def decodeBooleans(bytes: Array[Byte]): Either[CodecError, OwnedBooleans] =
    val values = new Array[Boolean](bytes.length)
    var index = 0
    while index < bytes.length do
      bytes(index) match
        case 0     => values(index) = false
        case 1     => values(index) = true
        case found =>
          return Left(
            CodecError.CorruptData("bytes", s"invalid bool byte ${found & 0xff} at element $index")
          )
      index += 1
    Right(OwnedBooleans.unsafe(values))

  private def decodeShorts(bytes: Array[Byte], count: Int, order: Endianness): OwnedShorts =
    val values = new Array[Short](count)
    var index = 0
    while index < count do
      values(index) = read16(bytes, index * 2, order).toShort
      index += 1
    OwnedShorts.unsafe(values)

  private def decodeInts(bytes: Array[Byte], count: Int, order: Endianness): OwnedInts =
    val values = new Array[Int](count)
    var index = 0
    while index < count do
      values(index) = read32(bytes, index * 4, order)
      index += 1
    OwnedInts.unsafe(values)

  private def decodeLongs(bytes: Array[Byte], count: Int, order: Endianness): OwnedLongs =
    val values = new Array[Long](count)
    var index = 0
    while index < count do
      values(index) = read64(bytes, index * 8, order)
      index += 1
    OwnedLongs.unsafe(values)

  private def decodeFloats(bytes: Array[Byte], count: Int, order: Endianness): OwnedFloats =
    val values = new Array[Float](count)
    var index = 0
    while index < count do
      values(index) = java.lang.Float.intBitsToFloat(read32(bytes, index * 4, order))
      index += 1
    OwnedFloats.unsafe(values)

  private def decodeDoubles(bytes: Array[Byte], count: Int, order: Endianness): OwnedDoubles =
    val values = new Array[Double](count)
    var index = 0
    while index < count do
      values(index) = java.lang.Double.longBitsToDouble(read64(bytes, index * 8, order))
      index += 1
    OwnedDoubles.unsafe(values)

  private def decodeComplex64(
      bytes: Array[Byte],
      componentCount: Int,
      order: Endianness
  ): OwnedComplex64 = OwnedComplex64.unsafe(decodeFloats(bytes, componentCount, order).values)

  private def decodeComplex128(
      bytes: Array[Byte],
      componentCount: Int,
      order: Endianness
  ): OwnedComplex128 = OwnedComplex128.unsafe(decodeDoubles(bytes, componentCount, order).values)

  private def encodeBooleans(values: Array[Boolean]): OwnedBytes =
    val bytes = new Array[Byte](values.length)
    var index = 0
    while index < values.length do
      bytes(index) = if values(index) then 1 else 0
      index += 1
    OwnedBytes.unsafe(bytes)

  private def encodeShorts(values: Array[Short], order: Endianness): OwnedBytes =
    val bytes = new Array[Byte](values.length * 2)
    var index = 0
    while index < values.length do
      write16(bytes, index * 2, values(index).toInt, order)
      index += 1
    OwnedBytes.unsafe(bytes)

  private def encodeInts(values: Array[Int], order: Endianness): OwnedBytes =
    val bytes = new Array[Byte](values.length * 4)
    var index = 0
    while index < values.length do
      write32(bytes, index * 4, values(index), order)
      index += 1
    OwnedBytes.unsafe(bytes)

  private def encodeLongs(values: Array[Long], order: Endianness): OwnedBytes =
    val bytes = new Array[Byte](values.length * 8)
    var index = 0
    while index < values.length do
      write64(bytes, index * 8, values(index), order)
      index += 1
    OwnedBytes.unsafe(bytes)

  private def encodeFloats(values: Array[Float], order: Endianness): OwnedBytes =
    val bits = new Array[Int](values.length)
    var index = 0
    while index < values.length do
      bits(index) = java.lang.Float.floatToRawIntBits(values(index))
      index += 1
    encodeInts(bits, order)

  private def encodeDoubles(values: Array[Double], order: Endianness): OwnedBytes =
    val bits = new Array[Long](values.length)
    var index = 0
    while index < values.length do
      bits(index) = java.lang.Double.doubleToRawLongBits(values(index))
      index += 1
    encodeLongs(bits, order)

  private def read16(bytes: Array[Byte], offset: Int, order: Endianness): Int = order match
    case Endianness.Little => unsigned(bytes(offset)) | unsigned(bytes(offset + 1)) << 8
    case Endianness.Big    => unsigned(bytes(offset)) << 8 | unsigned(bytes(offset + 1))

  private def read32(bytes: Array[Byte], offset: Int, order: Endianness): Int =
    var result = 0
    var byteIndex = 0
    while byteIndex < 4 do
      val source = order match
        case Endianness.Little => offset + byteIndex
        case Endianness.Big    => offset + 3 - byteIndex
      result |= unsigned(bytes(source)) << (byteIndex * 8)
      byteIndex += 1
    result

  private def read64(bytes: Array[Byte], offset: Int, order: Endianness): Long =
    var result = 0L
    var byteIndex = 0
    while byteIndex < 8 do
      val source = order match
        case Endianness.Little => offset + byteIndex
        case Endianness.Big    => offset + 7 - byteIndex
      result |= (unsigned(bytes(source)).toLong & 0xffL) << (byteIndex * 8)
      byteIndex += 1
    result

  private def write16(bytes: Array[Byte], offset: Int, value: Int, order: Endianness): Unit =
    order match
      case Endianness.Little =>
        bytes(offset) = value.toByte
        bytes(offset + 1) = (value >>> 8).toByte
      case Endianness.Big =>
        bytes(offset) = (value >>> 8).toByte
        bytes(offset + 1) = value.toByte

  private def write32(bytes: Array[Byte], offset: Int, value: Int, order: Endianness): Unit =
    var byteIndex = 0
    while byteIndex < 4 do
      val target = order match
        case Endianness.Little => offset + byteIndex
        case Endianness.Big    => offset + 3 - byteIndex
      bytes(target) = (value >>> (byteIndex * 8)).toByte
      byteIndex += 1

  private def write64(bytes: Array[Byte], offset: Int, value: Long, order: Endianness): Unit =
    var byteIndex = 0
    while byteIndex < 8 do
      val target = order match
        case Endianness.Little => offset + byteIndex
        case Endianness.Big    => offset + 7 - byteIndex
      bytes(target) = (value >>> (byteIndex * 8)).toByte
      byteIndex += 1

  private inline def unsigned(value: Byte): Int = value.toInt & 0xff
