package zarr4s

/** Pure byte-shuffle transform used by the common Zarr v2 filter. */
object Shuffle:
  def encode(decoded: OwnedBytes, elementSize: Int): Either[CodecError, OwnedBytes] =
    transform(decoded, elementSize, unshuffle = false)

  def decode(
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits,
      elementSize: Int
  ): Either[CodecError, OwnedBytes] =
    if expectedDecoded.toLong > limits.maxDecodedBytes.toLong then
      Left(CodecError.DecodedLimitExceeded(limits.maxDecodedBytes.toLong, expectedDecoded.toLong))
    else if expectedDecoded.toLong > Int.MaxValue.toLong then
      Left(CodecError.DecodedLimitExceeded(Int.MaxValue.toLong, expectedDecoded.toLong))
    else if expectedDecoded.toLong != encoded.length.toLong then
      Left(CodecError.InvalidDecodedLength(expectedDecoded.toLong, encoded.length.toLong))
    else transform(encoded, elementSize, unshuffle = true)

  private def transform(
      input: OwnedBytes,
      elementSize: Int,
      unshuffle: Boolean
  ): Either[CodecError, OwnedBytes] =
    if elementSize < 1 then
      Left(CodecError.CorruptData("shuffle", s"elementsize must be positive, found $elementSize"))
    else if elementSize == 1 || input.length == 0 then Right(OwnedBytes.copyOf(input.values))
    else if input.length % elementSize != 0 then
      Left(
        CodecError.CorruptData(
          "shuffle",
          s"payload length ${input.length} is not divisible by elementsize $elementSize"
        )
      )
    else
      val itemCount = input.length / elementSize
      val output = new Array[Byte](input.length)
      var byte = 0
      while byte < elementSize do
        var item = 0
        while item < itemCount do
          if unshuffle then output(item * elementSize + byte) = input(byte * itemCount + item)
          else output(byte * itemCount + item) = input(item * elementSize + byte)
          item += 1
        byte += 1
      Right(OwnedBytes.unsafe(output))
