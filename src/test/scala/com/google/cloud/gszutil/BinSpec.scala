package com.google.cloud.gszutil

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding._
import com.google.common.base.Charsets
import org.scalatest.FlatSpec


class BinSpec extends FlatSpec {
  "Decoder" should "unpack 2 byte binary integer" in {
    val data = Array[Byte](20.toByte, 140.toByte)
    val x = LongDecoder(2).get(data, 0)
    assert(x == 5260)
  }

  it should "unpack 4 byte integer" in {
    val data = Array[Byte](0.toByte,180.toByte, 25.toByte, 41.toByte)
    val decoder = LongDecoder(4)
    val x = decoder.get(data, 0)
    assert(x == 11802921)
  }

  it should "unpack 6 byte decimal" in {
    val data = Array[Byte](
      0.toByte,
      0.toByte,
      0.toByte,
      0.toByte,
      0x12.toByte,
      0x8C.toByte
    )

    System.out.println("Unpacked:\n"+PackedDecimal.hexValue(data))
    val decoder = DecimalDecoder(9,2)
    val x = decoder.get(data, 0)
    assert(x == BigDecimal(128L, 2))
  }

  it should "decode char" in {

    val decoder = StringDecoder(1)
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    val cb = CP1047.decode(buf)
    val x = cb.toString.toCharArray
    assert(x(uint(228.toByte)) == 'U')
    assert(x(uint(201.toByte)) == 'I')
    assert(decoder.get(Array(228.toByte), 0).sameElements("U".getBytes(Charsets.UTF_8)))
    assert(decoder.get(Array(201.toByte), 0).sameElements("I".getBytes(Charsets.UTF_8)))
  }
}
