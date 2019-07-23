/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gszutil


import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.google.cloud.gszutil.Decoding._
import com.google.common.base.Charsets
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, Decimal64ColumnVector, DecimalColumnVector, LongColumnVector}
import org.scalatest.FlatSpec
import com.ibm.jzos.fields.daa

class DecodingSpec extends FlatSpec {
  "Decoder" should "decode 2 byte binary integer" in {
    val f = new daa.BinarySignedIntL2Field(0)
    val a = Array[Byte](20.toByte, 140.toByte)
    val buf = ByteBuffer.wrap(a)
    val decoder = LongDecoder(2)
    val col = decoder.columnVector(1)
    System.out.println(Binary.binValue(a))
    val testValues = Seq(-32768, -1, 0, 1, 5260, 32767)
    for (x <- testValues){
      f.putInt(x, a,0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "decode 4 byte binary integer" in {
    val f = new daa.BinarySignedIntL4Field(0)
    val a = new Array[Byte](4)
    val buf = ByteBuffer.wrap(a)
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)

    val testValues = Seq(-174, -1, 1, 0, 11802921, Int.MinValue, Int.MaxValue)
    for (x <- testValues) {
      f.putInt(x, a, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "decode 8 byte binary long" in {
    val f = new daa.BinarySignedLongL8Field(0)
    val a = new Array[Byte](8)
    val buf = ByteBuffer.wrap(a)
    val decoder = LongDecoder(8)
    val col = decoder.columnVector(1)

    val testValues = Seq(Long.MinValue, -174, -1, 1, 0, Long.MaxValue)
    for (x <- testValues) {
      f.putLong(x, a, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "decode 4 byte binary unsigned integer" in {
    val f = new daa.BinaryUnsignedIntL4Field(0)
    val a = new Array[Byte](4)
    val buf = ByteBuffer.wrap(a)
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)

    val testValues = Seq(0, 1, Int.MaxValue)
    for (x <- testValues) {
      f.putInt(x, a, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "decode 8 byte binary unsigned long" in {
    val f = new daa.BinaryUnsignedLongL8Field(0)
    val a = new Array[Byte](8)
    val buf = ByteBuffer.wrap(a)
    val decoder = LongDecoder(8)
    val col = decoder.columnVector(1)

    val testValues = Seq(0, 1, Long.MaxValue)
    for (x <- testValues) {
      f.putLong(x, a, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "unpack 4 byte decimal" in {
    val f = new daa.PackedSignedIntP7Field(0)
    val a = Array[Byte](0x00.toByte,0x00.toByte,0x17.toByte, 0x4D.toByte)
    val buf = ByteBuffer.wrap(a)
    val decoder = Decimal64Decoder(7,0)
    val col = decoder.columnVector(1)
    val testValues = Seq(-9999999, 0, 1, 9999999)
    for (x <- testValues) {
      f.putInt(x, a, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "unpack 6 byte decimal" in {
    val f = new daa.PackedSignedLongP11Field(0)
    val a = new Array[Byte](6)
    val buf = ByteBuffer.wrap(a)
    val decoder = Decimal64Decoder(9,2)
    val col = decoder.columnVector(1)
    val testValues = Seq(-99999999999L, 0, 1, 128, 99999999999L)
    for (x <- testValues) {
      f.putLong(x, a, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "unpack 10 byte long" in {
    val f = new daa.PackedSignedLongP18Field(0)
    val a = new Array[Byte](10)
    val buf = ByteBuffer.wrap(a)
    val decoder = Decimal64Decoder(16,2)
    val col = decoder.columnVector(1)
    val testValues = Seq(-999999999999999999L, -100000000000000001L, 0, 1, 100000000000000001L, 999999999999999999L)
    for (x <- testValues) {
      f.putLong(x, a, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      System.out.println(s"$x:\n${Binary.binValue(a)}\n${PackedDecimal.hexValue(a)}")
      assert(col.asInstanceOf[LongColumnVector].vector(0) == x)
    }
  }

  it should "unpack 18 digit decimal" in {
    val len = PackedDecimal.sizeOf(16,2)
    System.out.println(s"$len")
    val a = Array.fill[Byte](len)(0x00.toByte)
    a(0) = 0x01.toByte
    a(len-1) = 0x1C.toByte
    val buf = ByteBuffer.wrap(a)

    System.out.println("Unpacked:\n"+PackedDecimal.hexValue(a))
    val unpackedLong = PackedDecimal.unpack(ByteBuffer.wrap(a), a.length)
    val expected = 100000000000000001L
    assert(unpackedLong == expected)

    val decoder = Decimal64Decoder(16,2)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)

    assert(col.asInstanceOf[Decimal64ColumnVector].vector(0) == expected)
  }

  it should "transcode EBCDIC" in {
    val test = Util.randString(10000)
    val in = test.getBytes(Decoding.CP1047)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq

    val got = in.map(Decoding.ebcdic2ascii)
    val n = got.length

    assert(n == expected.length)
    assert(got.sameElements(expected))
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
    val vec = col.asInstanceOf[BytesColumnVector].vector
    assert(vec.length == 1)
    val x = vec(0)
    val defaultBufferSize = 16384
    assert(x.length == defaultBufferSize)
    val chars = x.slice(0,2).toSeq
    val expected = "UI".getBytes(Charsets.UTF_8).toSeq
    assert(chars == expected)
  }

  it should "remove non-ascii characters" in {
    val a = (0 until 256)
      .map(x => (x, ebcdic2SafeUtf8(Array(x.toByte))))

    a.foreach{x => System.out.println(s"${x._1} -> '${x._2}'")}

    (
      (0 to 74) ++ (81 to 90) ++ (98 to 106) ++
      (112 to 120) ++ (138 to 144) ++ (154 to 160) ++ (170 to 172) ++
      (174 to 188) ++ (190 to 191) ++ (202 to 207) ++ (218 to 223) ++
      Seq(128, 225) ++ (234 to 239) ++ (234 to 239) ++ (250 to 255)
    ).foreach{i => assert(a(i)._2 == " ")}
  }

  it should "read dataset as string" in {
    val data = Seq("SELECT    ","1         ","FROM DUAL ")
      .mkString("")
      .getBytes(Charsets.UTF_8)
    val result = Util.records2string(data, 10, Charsets.UTF_8, "\n ")
    val expected =
      """SELECT
        | 1
        | FROM DUAL""".stripMargin
    assert(result == expected)
  }

  it should "unpack 31 digit decimal" in {
    val len = PackedDecimal.sizeOf(29,2)
    val a = Array.fill[Byte](len)(0x00.toByte)
    a(0) = 0x10.toByte
    a(len-1) = 0x1C.toByte

    System.out.println("Unpacked:\n"+PackedDecimal.hexValue(a))
    val decoder = DecimalDecoder(29,2)
    val col = decoder.columnVector(1)
    decoder.get(ByteBuffer.wrap(a), col, 0)
    val expectedDecimal = "10000000000000000000000000000.01"

    assert(col.asInstanceOf[DecimalColumnVector].vector.length == 1)
    val hiveDecimal = col.asInstanceOf[DecimalColumnVector].vector(0)
    val string = hiveDecimal.toString
    System.out.println(string)
    assert(string == expectedDecimal)
  }
}
