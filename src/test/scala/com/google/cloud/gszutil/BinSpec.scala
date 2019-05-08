package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding._
import org.scalatest.FlatSpec


class BinSpec extends FlatSpec {
  "Decoder" should "unpack 2 byte binary integer" in {
    val data = Array[Byte](20.toByte, 140.toByte)
    val x = IntDecoder(2, 0).get(data, 0)
    assert(x == 5260)
  }

  it should "unpack 4 byte integer" in {
    val data = Array[Byte](0.toByte,180.toByte, 25.toByte, 41.toByte)
    val decoder = IntDecoder(4, 0)
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
    val decoder = DecimalDecoder(9,2,0)
    val x = decoder.get(data, 0)
    assert(x == BigDecimal(128L, 2))
  }
}
