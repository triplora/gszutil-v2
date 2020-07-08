package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.time.LocalDate

import com.google.cloud.gszutil.Decoding.{Decimal64Decoder, IntAsDateDecoder, LongDecoder}
import com.google.cloud.gszutil.Encoding.{DateStringToBinaryEncoder, DecimalToBinaryEncoder, LongToBinaryEncoder, StringToBinaryEncoder}
import com.google.cloud.imf.gzos.Ebcdic
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

  "LongToBinaryEncoder" should "encode integer" in {
    val example = 1234L
    val encoder = LongToBinaryEncoder(4)

    val buf: Array[Byte] = encoder.encode(example)
    val decoder = LongDecoder(4)
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

}