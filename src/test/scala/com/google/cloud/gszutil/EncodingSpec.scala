package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.time.LocalDate

import com.google.cloud.gszutil.Decoding.{Decimal64Decoder, IntAsDateDecoder, LongDecoder}
import com.google.cloud.gszutil.Encoding._
import com.google.cloud.imf.gzos.{Ebcdic, PackedDecimal}
import org.apache.hadoop.hive.ql.exec.vector.{DateColumnVector, Decimal64ColumnVector, LongColumnVector}
import org.scalatest.flatspec.AnyFlatSpec

class EncodingSpec extends AnyFlatSpec {

  "StringToBinaryEncoder" should "encode ASCII string" in {
    val example = "abcd"
    val encoder = StringToBinaryEncoder(Ebcdic, example.length)
    val buf = encoder.encode(example)
    val decoded = new String(buf, Ebcdic.charset)
    assert(example.equals(decoded))
  }

  "StringToBinaryEncoder" should "throw an error when size higher than expected" in {
    assertThrows[RuntimeException]{
      StringToBinaryEncoder(Ebcdic, 2).encode("abcd")
    }
  }

  "StringToBinaryEncoder" should "encode string with length less than expected" in {
    val example = "abc"
    val res = StringToBinaryEncoder(Ebcdic, 4).encode(example)
    assert(res.length == 4)
    assert("abc".trim.equals(new String(res, Ebcdic.charset).trim))
  }

  "LongToBinaryEncoder" should "encode integer" in {
    val example = 1234L
    val encoder = LongToBinaryEncoder(4)
    val buf: Array[Byte] = encoder.encode(example)
    assert(buf.length == 4)

    val decoder = new LongDecoder(4)
    val col = decoder.columnVector(1)
    decoder.get(ByteBuffer.wrap(buf), col, 0)

    val decoded = col.asInstanceOf[LongColumnVector].vector(0)
    assert(example == decoded)
  }

  "DecimalToBinaryEncoder" should "encode decimal" in {
    val example = 1234L
    val precision = 4
    val scale = 2
    val encoder = DecimalToBinaryEncoder(precision, scale)
    val buf = encoder.encode(example)

    val decoder = Decimal64Decoder(precision, scale)
    val col = decoder.columnVector(1)
    decoder.get(ByteBuffer.wrap(buf), col, 0)

    val decoded = col.asInstanceOf[Decimal64ColumnVector].vector(0)
    assert(example == decoded)
  }

  "DateToBinaryEncoder" should "encode date" in {
    val date = "2020-07-08"
    val encoder = DateStringToBinaryEncoder()
    val encoded: Array[Byte] = encoder.encode(date)
    assert(encoded.length == 4)
    assert(encoded.exists(_ != 0))

    // decode encoded value
    val decoder = IntAsDateDecoder()
    val col = decoder.columnVector(1)
    decoder.get(ByteBuffer.wrap(encoded), col, 0)

    val l = col.asInstanceOf[DateColumnVector].vector(0)
    val d = LocalDate.ofEpochDay(l)

    assert(2020 == d.getYear)
    assert(7 == d.getMonthValue)
    assert(8 == d.getDayOfMonth)
  }

  "BytesToBinaryEncoder" should "encode" in {
    val example = Array[Byte](0x01, 0x02, 0x03, 0x04)
    assert(example == BytesToBinaryEncoder(4).encode(example))
  }

  it should "encode nulls" in {
    assert(StringToBinaryEncoder(Ebcdic, 10).encode(null).filter(_ != 0x00).isEmpty)
    assert(DateStringToBinaryEncoder().encode(null).filter(_ != 0x00).isEmpty)
    assert(LongToBinaryEncoder(4).encode(null).filter(_ != 0x00).isEmpty)

    val decimalEncoder = DecimalToBinaryEncoder(7, 2)
    val encoded = decimalEncoder.encode(null)
    val size = PackedDecimal.sizeOf(7, 2)
    assert(encoded.length == size)
    assert(decimalEncoder.encode(null).filter(_ != 0x00).isEmpty)
  }

  "packed decimal" should "encode" in {
    val s = 2
    val value = "153.99"
    var v1 = value.toDouble
    var scale = 0
    while (scale < s) {
      v1 *= 10d
      scale += 1
    }
    val l = v1.toLong
    assert(l == 15399L)
  }
}
