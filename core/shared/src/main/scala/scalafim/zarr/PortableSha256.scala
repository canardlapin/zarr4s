package scalafim.zarr

opaque type Sha256Hash = String

object Sha256Hash:
  def from(value: String): Either[ZarrError, Sha256Hash] =
    val normalized = value.toLowerCase
    if normalized.length == 64 &&
        normalized.forall(character => Character.digit(character, 16) >= 0)
    then Right(normalized)
    else Left(ZarrError.InvalidSelection("SHA-256 must contain 64 hexadecimal digits"))

  private[zarr] def unsafe(value: String): Sha256Hash = value

  extension (hash: Sha256Hash)
    inline def value: String = hash

/** Dependency-free SHA-256 for deterministic receipts on the JVM and Scala.js. */
object PortableSha256:
  private val initial = Array(
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
  )

  private val constants = Array(
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
  )

  def digest(bytes: OwnedBytes): Sha256Hash = digest(bytes.values)

  def digestUtf8(value: String): Sha256Hash = digest(value.getBytes("UTF-8"))

  def digest(input: Array[Byte]): Sha256Hash =
    val hash = java.util.Arrays.copyOf(initial, initial.length)
    val words = new Array[Int](64)
    var block = 0
    while block + 64 <= input.length do
      compress(input, block, hash, words)
      block += 64

    val remainder = input.length - block
    val tailLength = if remainder <= 55 then 64 else 128
    val tail = new Array[Byte](tailLength)
    Array.copy(input, block, tail, 0, remainder)
    tail(remainder) = 0x80.toByte
    val bitLength = input.length.toLong * 8L
    var lengthByte = 0
    while lengthByte < 8 do
      tail(tail.length - 1 - lengthByte) = (bitLength >>> (lengthByte * 8)).toByte
      lengthByte += 1
    block = 0
    while block < tail.length do
      compress(tail, block, hash, words)
      block += 64

    val result = new java.lang.StringBuilder(64)
    hash.foreach: word =>
      val hex = java.lang.Integer.toHexString(word)
      var padding = hex.length
      while padding < 8 do
        result.append('0')
        padding += 1
      result.append(hex)
    Sha256Hash.unsafe(result.toString)

  private def compress(
      input: Array[Byte],
      offset: Int,
      hash: Array[Int],
      words: Array[Int]
  ): Unit =
    var index = 0
    while index < 16 do
      val wordOffset = offset + index * 4
      words(index) =
        (input(wordOffset) & 0xff) << 24 |
        (input(wordOffset + 1) & 0xff) << 16 |
        (input(wordOffset + 2) & 0xff) << 8 |
        (input(wordOffset + 3) & 0xff)
      index += 1
    while index < 64 do
      val s0 = java.lang.Integer.rotateRight(words(index - 15), 7) ^
        java.lang.Integer.rotateRight(words(index - 15), 18) ^ (words(index - 15) >>> 3)
      val s1 = java.lang.Integer.rotateRight(words(index - 2), 17) ^
        java.lang.Integer.rotateRight(words(index - 2), 19) ^ (words(index - 2) >>> 10)
      words(index) = words(index - 16) + s0 + words(index - 7) + s1
      index += 1

    var a = hash(0)
    var b = hash(1)
    var c = hash(2)
    var d = hash(3)
    var e = hash(4)
    var f = hash(5)
    var g = hash(6)
    var h = hash(7)
    index = 0
    while index < 64 do
      val sum1 = java.lang.Integer.rotateRight(e, 6) ^
        java.lang.Integer.rotateRight(e, 11) ^ java.lang.Integer.rotateRight(e, 25)
      val choose = (e & f) ^ (~e & g)
      val temporary1 = h + sum1 + choose + constants(index) + words(index)
      val sum0 = java.lang.Integer.rotateRight(a, 2) ^
        java.lang.Integer.rotateRight(a, 13) ^ java.lang.Integer.rotateRight(a, 22)
      val majority = (a & b) ^ (a & c) ^ (b & c)
      val temporary2 = sum0 + majority
      h = g
      g = f
      f = e
      e = d + temporary1
      d = c
      c = b
      b = a
      a = temporary1 + temporary2
      index += 1
    hash(0) += a
    hash(1) += b
    hash(2) += c
    hash(3) += d
    hash(4) += e
    hash(5) += f
    hash(6) += g
    hash(7) += h
