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


class DecodingSpec extends FlatSpec {
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

  it should "unpack 4 byte negative integer" in {
    val buf = ByteBuffer.wrap(Array[Byte](0x00.toByte,0x00.toByte,0x17.toByte, 0x4D.toByte))
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    assert(col.asInstanceOf[LongColumnVector].vector(0) == -174)
  }

  it should "unpack 6 byte decimal" in {
    val len = PackedDecimal.sizeOf(9,2)
    assert(len == 6)
    val a = Array.fill[Byte](len)(0)
    a(len-2) = 0x12.toByte
    a(len-1) = 0x8C.toByte
    val expected = 128L

    val unpackedLong = PackedDecimal.unpack(ByteBuffer.wrap(a), a.length)
    assert(unpackedLong == expected)
    System.out.println("Unpacked:\n"+PackedDecimal.hexValue(a))
    assert(PackedDecimal.sizeOf(9,2) == 6)
    val decoder = Decimal64Decoder(9,2)
    val col = decoder.columnVector(1)
    decoder.get(ByteBuffer.wrap(a), col, 0)

    val vec = col.asInstanceOf[Decimal64ColumnVector].vector
    val got = vec(0)
    assert(got == expected)
  }

  it should "unpack 18 digit decimal" in {
    val len = PackedDecimal.sizeOf(16,2)
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
