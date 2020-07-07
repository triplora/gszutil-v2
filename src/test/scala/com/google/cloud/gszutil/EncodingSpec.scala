package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.{Decimal64Decoder, LongDecoder}
import com.google.cloud.gszutil.Encoding.{DateStringToBinaryEncoder, DecimalToBinaryEncoder, LongToBinaryEncoder, StringToBinaryEncoder}
import com.google.cloud.imf.gzos.Ebcdic
import org.apache.hadoop.hive.ql.exec.vector.{Decimal64ColumnVector, LongColumnVector}
import org.scalatest.flatspec.AnyFlatSpec

class EncodingSpec extends AnyFlatSpec {

  "StringToBinaryEncoder" should "encode ASCII string" in {
    val example = "abcd"
    val encoder = StringToBinaryEncoder(Ebcdic, example.length)

    val buf = encoder.encode(example)
    val decoded = new String(buf.array(), Ebcdic.charset)
    assert(example.equals(decoded))
  }

  "LongToBinaryEncoder" should "encode integer" in {
    val example = 1234L
    val encoder = LongToBinaryEncoder(4)

    val buf = encoder.encode(example)
    buf.array().foreach(System.out.println(_))

    val decoder = LongDecoder(4)
    val col = decoder.columnVector(1)
    buf.flip()
    decoder.get(buf, col, 0)

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
    buf.flip()
    decoder.get(buf, col, 0)

    val decoded = col.asInstanceOf[Decimal64ColumnVector].vector(0)
    assert(example == decoded)
  }

  "DateToBinaryEncoder" should "encode date" in {
    val date = "2020-07-15"
    val encoder = DateStringToBinaryEncoder()
    encoder.encode(date)
  }

}