package com.google.cloud.gszutil

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding._
import org.scalatest.FlatSpec


class BinSpec extends FlatSpec {
  "Decoder" should "unpack 2 byte binary integer" in {
    val data = Array[Byte](20.toByte, 140.toByte)
    val decoder = new IntDecoder2
    val buf = ByteBuffer.allocate(56)
    buf.put(data)
    buf.flip()
    val x = decoder.decode(buf)
    assert(x == 5260)
  }

  it should "unpack 4 byte integer" in {
    val data = Array[Byte](0.toByte,180.toByte, 25.toByte, 41.toByte)
    val buf = ByteBuffer.allocate(56)
    buf.put(data)
    buf.flip()
    val decoder = new IntDecoder4
    val x = decoder.decode(buf)
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

    val buf = ByteBuffer.allocate(6)
    buf.put(data)
    buf.flip()
    val decoder = new NumericDecoder
    val x = decoder.decode(buf)
    assert(x == BigDecimal(128L,2))
  }
}
