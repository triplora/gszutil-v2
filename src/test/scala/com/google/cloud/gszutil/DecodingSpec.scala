/*
 * Copyright 2019 Google LLC All Rights Reserved.
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

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.cloud.gszutil.Decoding.{Decimal64Decoder, Decoder, LongDecoder, StringAsDateDecoder, StringAsDecimalDecoder, StringAsIntDecoder, StringDecoder, ebcdic2ASCIIString, validAscii}
import com.google.cloud.gszutil.io.ZReader
import com.google.cloud.gszutil.io.ZReader.readColumn
import com.google.common.base.Charsets
import com.ibm.jzos.fields.daa
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, DateColumnVector, Decimal64ColumnVector, DecimalColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer

class DecodingSpec extends FlatSpec {
  "Decoder" should "decode 2 byte binary integer" in {
    val field = new daa.BinarySignedIntL2Field(0)
    val exampleData = Array[Byte](20.toByte, 140.toByte)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = LongDecoder(2)
    val col = decoder.columnVector(1)
    val minTwoByteInt = -32768
    val maxTwoByteInt = 32767
    val testValues = Seq(minTwoByteInt, -1, 0, 1, 5260, maxTwoByteInt)
    for (testValue <- testValues){
      field.putInt(testValue, exampleData,0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "decode 4 byte binary integer" in {
    val field = new daa.BinarySignedIntL4Field(0)
    val exampleData = new Array[Byte](4)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)

    val minFourByteInt = Int.MinValue
    val maxFourByteInt = Int.MaxValue

    val testValues = Seq(minFourByteInt, -174, -1, 1, 0, 11802921, maxFourByteInt)
    for (testValue <- testValues) {
      field.putInt(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "decode 8 byte binary long" in {
    val field = new daa.BinarySignedLongL8Field(0)
    val exampleData = new Array[Byte](8)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = LongDecoder(8)
    val col = decoder.columnVector(1)
    val minEightByteInteger = Long.MinValue
    val maxEightByteInteger = Long.MaxValue
    val testValues = Seq(minEightByteInteger, -174, -1, 1, 0, maxEightByteInteger)
    for (testValue <- testValues) {
      field.putLong(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "decode 4 byte binary unsigned integer" in {
    val field = new daa.BinaryUnsignedIntL4Field(0)
    val exampleData = new Array[Byte](4)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)
    val testValues = Seq(0, 1, Int.MaxValue)
    for (testValue <- testValues) {
      field.putInt(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "decode 8 byte binary unsigned long" in {
    val field = new daa.BinaryUnsignedLongL8Field(0)
    val exampleData = new Array[Byte](8)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = LongDecoder(8)
    val col = decoder.columnVector(1)
    val testValues = Seq(0, 1, Long.MaxValue)
    for (testValue <- testValues) {
      field.putLong(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "unpack 4 byte decimal" in {
    val field = new daa.PackedSignedIntP7Field(0)
    val exampleData = Array[Byte](0x00.toByte,0x00.toByte,0x17.toByte, 0x4D.toByte)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = Decimal64Decoder(7,0)
    val col = decoder.columnVector(1)
    // 4 bytes contains 8 half-bytes, with 1 reserved for sign
    // so this field type supports a maximum of 7 digits
    val min7DigitInt = -9999999
    val max7DigitInt = 9999999
    val testValues = Seq(min7DigitInt, 0, 1, max7DigitInt)
    for (testValue <- testValues) {
      field.putInt(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      assert(col.asInstanceOf[LongColumnVector].vector(0) == testValue)
    }
  }

  it should "unpack 6 byte decimal" in {
    val field = new daa.PackedSignedLongP11Field(0)
    val exampleData = new Array[Byte](6)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = Decimal64Decoder(9,2)
    val col = decoder.columnVector(1)
    val w = new HiveDecimalWritable()
    // 6 bytes contains 12 half-bytes, with 1 reserved for sign
    // so this field type supports a maximum of 11 digits
    val min11DigitValue = -99999999999L
    val max11DigitValue = 99999999999L
    val testValues = Seq(min11DigitValue, 0, 1, 128, max11DigitValue)
    for (testValue <- testValues) {
      field.putLong(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      val got = col.asInstanceOf[LongColumnVector].vector(0)
      assert(got == testValue)
      w.setFromLongAndScale(testValue, 2)
      val fromHiveDecimal = w.serialize64(2)
      assert(fromHiveDecimal == testValue)
    }
  }

  it should "unpack 10 byte long" in {
    val field = new daa.PackedSignedLongP18Field(0)
    val exampleData = new Array[Byte](10)
    val buf = ByteBuffer.wrap(exampleData)
    val decoder = Decimal64Decoder(16,2)
    val col = decoder.columnVector(1)
    // 10 bytes contains 20 half-bytes, with 1 reserved for sign
    // but the first half-byte is not usable
    // so this field type supports a maximum of 18 digits
    val min18Digit = -999999999999999999L
    val max18Digit = 999999999999999999L
    val negative = -100000000000000001L
    val positive = 100000000000000001L
    val testValues = Seq(min18Digit, negative, 0, 1, positive, max18Digit)
    val w = new HiveDecimalWritable()
    for (testValue <- testValues) {
      field.putLong(testValue, exampleData, 0)
      buf.clear()
      decoder.get(buf, col, 0)
      val got = col.asInstanceOf[LongColumnVector].vector(0)
      assert(got == testValue)
      w.setFromLongAndScale(testValue, 2)
      val fromHiveDecimal = w.serialize64(2)
      assert(fromHiveDecimal == testValue)
    }
  }

  it should "unpack 18 digit decimal" in {
    val len = PackedDecimal.sizeOf(16,2)
    val exampleData = Array.fill[Byte](len)(0x00.toByte)
    exampleData(0) = 0x01.toByte
    exampleData(len-1) = 0x1C.toByte
    val buf = ByteBuffer.wrap(exampleData)
    val unpackedLong = PackedDecimal.unpack(ByteBuffer.wrap(exampleData), exampleData.length)
    val expected = 100000000000000001L
    assert(unpackedLong == expected)
    val decoder = Decimal64Decoder(16,2)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    val got = col.asInstanceOf[Decimal64ColumnVector].vector(0)
    assert(got == expected)
    val w = new HiveDecimalWritable()
    w.setFromLongAndScale(expected, 2)
    val fromHiveDecimal = w.serialize64(2)
    assert(fromHiveDecimal == expected)
  }

  it should "transcode EBCDIC" in {
    val test = Util.randString(10000)
    val in = test.getBytes(Decoding.CP1047)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq
    val got = in.map(Decoding.ebcdic2utf8byte)
    assert(got.length == expected.length)
    assert(got.sameElements(expected))
  }

  it should "decode char" in {
    assert(Decoding.ebcdic2utf8byte(228.toByte) == "U".getBytes(Charsets.UTF_8).head)
    assert(Decoding.ebcdic2utf8byte(201.toByte) == "I".getBytes(Charsets.UTF_8).head)
    assert(Decoding.e2a(173.toByte) == "[".getBytes(Charsets.UTF_8).head)
    assert(Decoding.e2a(189.toByte) == "]".getBytes(Charsets.UTF_8).head)
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

  it should "decode string as int" in {
    val examples = Seq((" 0", 0), ("00", 0), ("08",8), ("42",42))
    val decoder = StringAsIntDecoder(2)
    val col = decoder.columnVector(1)
    for ((a,b) <- examples){
      val buf = Decoding.EBCDIC1.encode(a)
      decoder.get(buf, col, 0)
      val vec: Array[Long] = col.asInstanceOf[LongColumnVector].vector
      assert(vec(0) == b)
    }
  }

  it should "decode string as date" in {
    val exampleDate = "02/01/2020"
    val fmt = DateTimeFormatter.ofPattern("MM/dd/yyyy")
    val ld = LocalDate.from(fmt.parse(exampleDate))
    val examples = Seq(
      (exampleDate, ld.toEpochDay, false),
      ("00/00/0000", -1L, true)
    )
    val decoder = StringAsDateDecoder(10, "MM/DD/YYYY")
    val col = decoder.columnVector(examples.length)
    val vec: Array[Long] = col.asInstanceOf[DateColumnVector].vector
    val isNull: Array[Boolean] = col.asInstanceOf[DateColumnVector].isNull
    for (i <- examples.indices){
      val (a,b,c) = examples(i)
      decoder.get(Decoding.EBCDIC1.encode(a), col, i)
      assert(vec(i) == b)
      assert(isNull(i) == c)
    }
  }

  it should "decode string as decimal" in {
    val examples = Seq(("0000004.82", 482))
    val decoder = StringAsDecimalDecoder(10,9,2)
    val col = decoder.columnVector(1)
    for ((a,b) <- examples){
      val buf = Decoding.EBCDIC1.encode(a)
      decoder.get(buf, col, 0)
      val vec: Array[Long] = col.asInstanceOf[LongColumnVector].vector
      assert(vec(0) == b)
    }
  }

  it should "remove non-ascii characters" in {
    // Map all possible byte values to ASCII
    val exampleData: Seq[(Int,String)] = (0 until 256)
      .map(x => (x, ebcdic2ASCIIString(Array(x.toByte))))

    // The ranges below are EBCDIC byte values that can't be mapped to ASCII,
    // determined by printing the converted values
    val unmappableEBCDICBytes: Seq[Int] =
      (0 to 74) ++ (81 to 90) ++ (98 to 106) ++
      (112 to 120) ++ (138 to 144) ++ (154 to 160) ++ (170 to 172) ++
      (174 to 185) ++ (190 to 191) ++ (202 to 207) ++ (218 to 223) ++
      Seq(128, 188, 225) ++ (234 to 239) ++ (234 to 239) ++ (250 to 255)

    unmappableEBCDICBytes.foreach{i =>
      val decodedChar = exampleData(i)._2
      assert(decodedChar == " ")
    }
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

  it should "decode example with cast" in {
    val example =
      "US.000000001.000100003.07.02/01/2020.02/09/2020.0000002.10.WK. .02/07/2020"

    val decoders = Array[Decoder](
      StringDecoder(2),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(9),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(9),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(2),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY"),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY"),
      StringDecoder(1,filler = true),
      StringAsDecimalDecoder(10,9,2),
      StringDecoder(1,filler = true),
      StringDecoder(2),
      StringDecoder(1,filler = true),
      StringDecoder(1),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY")
    )

    val cols = decoders.map(_.columnVector(8))
    val buf = ByteBuffer.wrap(example.getBytes(Decoding.EBCDIC1))

    // Print field
    //val copy = buf.asReadOnlyBuffer()
    //val dec = Decoding.EBCDIC1.newDecoder()

    var i = 0
    var pos = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)

      // Print field
      //val a = ByteBuffer.allocate(d.size)
      //copy.get(a.array())
      //System.out.println(s"$i '${dec.decode(a).toString}' $d")

      readColumn(buf, d, col, 0)
      pos += d.size

      assert(buf.position == pos)
      i += 1
    }

    val dcv = cols(12).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")
  }

  it should "decode bigger example with cast" in {
    val example =
      """US.000000001.000100003.07.02/01/2020.02/09/2020.0000002.10.WK. .02/07/2020
        |US.000000001.000100176.07.02/01/2020.02/09/2020.0000003.73.WK. .02/07/2020
        |US.000000001.000100220.07.02/01/2020.02/09/2020.0000002.22.WK. .02/07/2020
        |US.000000001.000100276.07.02/01/2020.02/09/2020.0000002.05.WK. .02/07/2020
        |US.000000001.000100737.07.02/01/2020.02/09/2020.0000002.68.WK. .02/07/2020
        |US.000000001.000100777.07.02/01/2020.02/09/2020.0000005.85.WK. .02/07/2020
        |US.000000001.000101239.07.02/01/2020.02/09/2020.0000001.78.WK. .02/07/2020
        |US.000000001.000101246.07.02/01/2020.02/09/2020.0000003.08.WK. .02/07/2020""".stripMargin

    val data = example.lines.foldLeft(new ArrayBuffer[Byte]()){(a,b) =>
      a.appendAll(b.getBytes(Decoding.EBCDIC1));a
    }.toArray

    val decoders = Array[Decoder](
      StringDecoder(2),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(9),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(9),
      StringDecoder(1,filler = true),
      StringAsIntDecoder(2),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY"),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY"),
      StringDecoder(1,filler = true),
      StringAsDecimalDecoder(10,9,2),
      StringDecoder(1,filler = true),
      StringDecoder(2),
      StringDecoder(1,filler = true),
      StringDecoder(1),
      StringDecoder(1,filler = true),
      StringAsDateDecoder(10, "MM/DD/YYYY")
    )

    val cols = decoders.map(_.columnVector(8))
    val buf = ByteBuffer.wrap(data)
    ZReader.readBatch(buf, decoders, cols, 8, 74, ByteBuffer.allocate(74))

    // Print field
    //val copy = buf.asReadOnlyBuffer()
    //val dec = Decoding.EBCDIC1.newDecoder()

    // reset buffer to replay the data
    buf.clear
    var i = 0
    var pos = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)

      // Print field
      //val a = ByteBuffer.allocate(d.size)
      //copy.get(a.array())
      //System.out.println(s"$i '${dec.decode(a).toString}' $d")

      readColumn(buf, d, col, 0)
      pos += d.size

      assert(buf.position == pos)
      i += 1
    }

    val dcv = cols(12).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")
  }
}
