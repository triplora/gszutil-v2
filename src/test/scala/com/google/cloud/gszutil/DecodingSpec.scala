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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.cloud.gszutil.Decoding.{Decimal64Decoder, DecimalAsStringDecoder, IntegerAsDateDecoder, LongAsStringDecoder, LongDecoder, NullableStringDecoder, StringAsDateDecoder, StringAsDecimalDecoder, StringAsIntDecoder, StringDecoder}
import com.google.cloud.gszutil.Encoding.DecimalToBinaryEncoder
import com.google.cloud.gszutil.io.ZReader
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.FieldType
import com.google.cloud.imf.gzos.{Binary, Ebcdic, PackedDecimal, Util}
import com.google.common.base.Charsets
import com.ibm.jzos.fields.daa
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, DateColumnVector, Decimal64ColumnVector, LongColumnVector}
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.scalatest.flatspec.AnyFlatSpec

class DecodingSpec extends AnyFlatSpec {
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

  it should "null packed decimal" in {
    val len = PackedDecimal.sizeOf(16,2)
    val exampleData = Array.fill[Byte](len)(0x00.toByte)
    val buf = ByteBuffer.wrap(exampleData)
    assertThrows[IllegalArgumentException](PackedDecimal.unpack(ByteBuffer.wrap(exampleData), exampleData.length))
    val decoder = Decimal64Decoder(16,2)
    val col = decoder.columnVector(1)
    decoder.get(buf, col, 0)
    val dcv = col.asInstanceOf[Decimal64ColumnVector]
    assert(!dcv.noNulls)
    assert(dcv.isNull(0))
  }

  it should "invalid packed decimal sign" in {
    val len = PackedDecimal.sizeOf(16,2)
    val exampleData = Array.fill[Byte](len)(0x00.toByte)
    exampleData.update(len-2, 0x12)
    exampleData.update(len-1, 0x10)
    val buf = ByteBuffer.wrap(exampleData)
    assertThrows[IllegalArgumentException](PackedDecimal.unpack(buf, len))
  }

  it should "invalid packed decimal digit" in {
    val len = PackedDecimal.sizeOf(16,2)
    val exampleData = Array.fill[Byte](len)(0x00.toByte)
    exampleData.update(len-2, 0xFF.toByte)
    exampleData.update(len-1, 0xFC.toByte)
    val buf = ByteBuffer.wrap(exampleData)
    assertThrows[IllegalArgumentException](PackedDecimal.unpack(buf, len))
  }

  it should "transcode EBCDIC" in {
    val test = Util.randString(10000)
    val in = test.getBytes(Ebcdic.charset)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq
    val got = in.map(Ebcdic.decodeByte)
    assert(got.length == expected.length)
    assert(got.sameElements(expected))
  }

  it should "decode char" in {
    assert(Ebcdic.decodeByte(228.toByte) == "U".getBytes(Charsets.UTF_8).head)
    assert(Ebcdic.decodeByte(201.toByte) == "I".getBytes(Charsets.UTF_8).head)
    assert(Ebcdic.decodeByte(173.toByte) == "[".getBytes(Charsets.UTF_8).head)
    assert(Ebcdic.decodeByte(189.toByte) == "]".getBytes(Charsets.UTF_8).head)
  }

  it should "decode string" in {
    val buf = ByteBuffer.wrap(Array[Byte](228.toByte,201.toByte))
    val decoder = new StringDecoder(Ebcdic,2)
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
    val decoder = new StringAsIntDecoder(Ebcdic,2)
    val col = decoder.columnVector(1)
    for ((a,b) <- examples){
      val buf = Ebcdic.charset.encode(a)
      decoder.get(buf, col, 0)
      val vec: Array[Long] = col.asInstanceOf[LongColumnVector].vector
      assert(vec(0) == b)
    }
  }

  it should "decode string as date 1" in {
    val exampleDate = "02/01/2020"
    val fmt = DateTimeFormatter.ofPattern("MM/dd/yyyy")
    val ld = LocalDate.from(fmt.parse(exampleDate))
    val examples = Seq(
      (exampleDate, ld.toEpochDay, false),
      ("00/00/0000", -1L, true)
    )
    val decoder = new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY")
    val col = decoder.columnVector(examples.length)
    val vec: Array[Long] = col.asInstanceOf[DateColumnVector].vector
    val isNull: Array[Boolean] = col.asInstanceOf[DateColumnVector].isNull
    for (i <- examples.indices){
      val (a,b,c) = examples(i)
      decoder.get(Ebcdic.charset.encode(a), col, i)
      assert(vec(i) == b)
      assert(isNull(i) == c)
    }
  }

  it should "decode string as date 2" in {
    val exampleDate = "20/02/01"
    val fmt = DateTimeFormatter.ofPattern("yy/MM/dd")
    val ld = LocalDate.from(fmt.parse(exampleDate))
    val examples = Seq(
      (exampleDate, ld.toEpochDay, false),
    )
    val decoder = new StringAsDateDecoder(Ebcdic, 8, "yy/MM/dd")
    val col = decoder.columnVector(examples.length)
    val vec: Array[Long] = col.asInstanceOf[DateColumnVector].vector
    val isNull: Array[Boolean] = col.asInstanceOf[DateColumnVector].isNull
    for (i <- examples.indices){
      val (a,b,c) = examples(i)
      decoder.get(Ebcdic.charset.encode(a), col, i)
      assert(vec(i) == b)
      assert(isNull(i) == c)
    }
  }

  it should "decode string as decimal" in {
    val examples = Seq(("0000004.82", 482))
    val decoder = new StringAsDecimalDecoder(Ebcdic,10,9,2)
    val col = decoder.columnVector(1)
    for ((a,b) <- examples){
      val buf = Ebcdic.charset.encode(a)
      decoder.get(buf, col, 0)
      val vec: Array[Long] = col.asInstanceOf[LongColumnVector].vector
      assert(vec(0) == b)
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
      new StringDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,9),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,9),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY"),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY"),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDecimalDecoder(Ebcdic,10,9,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringDecoder(Ebcdic,1),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY")
    )

    val cols = decoders.map(_.columnVector(8))
    val buf = ByteBuffer.wrap(example.getBytes(Ebcdic.charset))

    var i = 0
    var pos = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)

      d.get(buf, col, 0)
      pos += d.size

      assert(buf.position() == pos)
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
    val decoders = Array[Decoder](
      new StringDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,9),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,9),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsIntDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY"),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY"),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDecimalDecoder(Ebcdic,10,9,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringDecoder(Ebcdic,2),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringDecoder(Ebcdic,1),
      new StringDecoder(Ebcdic,1,filler = true),
      new StringAsDateDecoder(Ebcdic,10, "MM/DD/YYYY")
    )

    val batchSize = 8
    val cols = decoders.map(_.columnVector(batchSize))
    val buf = TestUtil.getBytes("mload0.dat")
    val lrecl = buf.array.length / batchSize
    val rBuf = ByteBuffer.allocate(lrecl)
    val errBuf = ByteBuffer.allocate(lrecl)
    val (rowId,errCt) = ZReader.readBatch(buf, decoders, cols, batchSize, lrecl, rBuf, errBuf)
    assert(rowId == batchSize)
    assert(errCt == 0)

    // reset buffer to replay the data
    buf.clear
    var i = 0
    var pos = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)

      d.get(buf, col, 0)
      pos += d.size

      assert(buf.position() == pos)
      i += 1
    }

    val dcv = cols(12).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")

    val strCol = cols.head.asInstanceOf[BytesColumnVector]
    val strCol2 = cols(14).asInstanceOf[BytesColumnVector]
    var j = 0
    while (j < batchSize){
      val a = strCol.vector(j)
      val start = strCol.start(j)
      val len = strCol.length(j)
      val s = new String(a,start,len,Charsets.UTF_8)
      assert(s == "US", s"row $j")

      val a2 = strCol2.vector(j)
      val start2 = strCol2.start(j)
      val len2 = strCol2.length(j)
      val s2 = new String(a2,start2,len2,Charsets.UTF_8)
      assert(s2 == "WK", s"row $j")
      j += 1
    }
  }

  it should "cast integer to date" in {
    val b = Field.newBuilder
      .setTyp(FieldType.INTEGER)
      .setCast(FieldType.DATE)
      .setFormat("YYMMDD")
      .build
    val decoder = Decoding.getDecoder(b, Utf8)
    assert(decoder.isInstanceOf[IntegerAsDateDecoder])
  }

  it should "nullif" in {
    val examples = Seq(
      (Array[Byte](0x5b,0x5b,0x4a,0x4a,0,1,2,3),true),
      (Array[Byte](0x5b,0x5b,0,0,0,1,2,3),false)
    )

    val nullIf = Array[Byte](0x5b,0x5b,0x4a,0x4a)
    Ebcdic.decodeBytes(nullIf)

    for ((example,expected) <- examples) {
      val buf = ByteBuffer.wrap(example)
      val decoder = new NullableStringDecoder(Ebcdic,4, nullIf)
      val bcv = decoder.columnVector(1)
      decoder.get(buf, bcv, 0)
      assert(bcv.isNull(0) == expected, "should be null")
    }
  }

  it should "cast decimal(5, 2) to string" in {
    // prepare input
    val example = 3L
    val precision = 5
    val scale = 2
    val encoder = DecimalToBinaryEncoder(precision - scale, scale)
    val decimalInputBuf = ByteBuffer.wrap(encoder.encode(example))

    // decode as String
    val decoder = DecimalAsStringDecoder(precision - scale, scale, (precision - scale) * 2, Ebcdic, filler = false)
    val col = decoder.columnVector(decoder.size).asInstanceOf[BytesColumnVector]
    decoder.get(decimalInputBuf, col, 0)

    val a = col.vector(0)
    val start = col.start(0)
    val len = col.length(0)
    System.out.println(a)
    System.out.println(start)
    System.out.println(len)
    val s = new String(a,start,len, Utf8.charset)
    assert(s == "0.03")
  }

  it should "cast integer to string" in {
    val example = 1234567
    val binary = Binary.encode(example, 4)
    val decoder = LongAsStringDecoder(Ebcdic, 4, 8, filler = false)
    val col = decoder.columnVector(decoder.size).asInstanceOf[BytesColumnVector]
    decoder.get(ByteBuffer.wrap(binary), col, 0)

    val a = col.vector(0)
    val start = col.start(0)
    val len = col.length(0)
    System.out.println(a)
    System.out.println(start)
    System.out.println(len)
    val s = new String(a,start,len, Utf8.charset)
    assert(s == "1234567")
  }

}
