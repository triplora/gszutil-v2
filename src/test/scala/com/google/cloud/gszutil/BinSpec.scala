package com.google.cloud.gszutil


import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding._
import com.google.common.base.Charsets
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, DecimalColumnVector, LongColumnVector}
import org.scalatest.FlatSpec


class BinSpec extends FlatSpec {
  "Decoder" should "unpack 2 byte binary integer" in {
    val buf = ByteBuffer.wrap(Array[Byte](20.toByte, 140.toByte))
    val decoder = LongDecoder(2)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    assert(col.asInstanceOf[LongColumnVector].vector(0) == 5260)
  }

  it should "unpack 4 byte integer" in {
    val buf = ByteBuffer.wrap(Array[Byte](0.toByte,180.toByte, 25.toByte, 41.toByte))
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    assert(col.asInstanceOf[LongColumnVector].vector(0) == 11802921)
  }

  it should "unpack 6 byte decimal" in {
    val buf = ByteBuffer.wrap(Array[Byte](
      0.toByte,
      0.toByte,
      0.toByte,
      0.toByte,
      0x12.toByte,
      0x8C.toByte
    ))

    System.out.println("Unpacked:\n"+PackedDecimal.hexValue(buf.array()))
    val decoder = DecimalDecoder(9,2)
    val col = decoder.columnVector(1)
    decoder.get(buf,col, 0)

    assert(col.asInstanceOf[DecimalColumnVector].vector(0).getHiveDecimal == HiveDecimal.create(BigDecimal(128L, 2).bigDecimal))
  }

  it should "decode char" in {
    assert(Decoding.ebcdic2ascii(228.toByte) == "U".getBytes(Charsets.UTF_8).head)
    assert(Decoding.ebcdic2ascii(201.toByte) == "I".getBytes(Charsets.UTF_8).head)
  }

  it should "decode string" in {
    val buf = ByteBuffer.wrap(Array[Byte](228.toByte,201.toByte))
    val decoder = StringDecoder(2)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    assert(col.asInstanceOf[BytesColumnVector].vector(0).sameElements("UI".getBytes(Charsets.UTF_8)))
  }
}
