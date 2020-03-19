package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.google.cloud.gszutil.VartextDecoding.{VartextStringAsDateDecoder, VartextStringAsDecimalDecoder, VartextStringAsIntDecoder, VartextStringDecoder}
import com.google.cloud.gszutil.io.ZReader
import com.google.cloud.gszutil.io.ZReader.readColumn
import com.google.cloud.gzos.Ebcdic
import com.google.common.base.Charsets
import com.google.common.io.Resources
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, DateColumnVector, Decimal64ColumnVector}
import org.scalatest.FlatSpec

class VartextSpec extends FlatSpec {
  private def exampleDecoders(delim: String, transcoder: Transcoder): Array[Decoder] = {
    val delimiter = delim.getBytes(transcoder.charset)
    Array[Decoder](
      new VartextStringDecoder(delimiter,transcoder,2),
      new VartextStringAsIntDecoder(delimiter,transcoder,9),
      new VartextStringAsIntDecoder(delimiter,transcoder,9),
      new VartextStringAsIntDecoder(delimiter,transcoder,2),
      new VartextStringAsDateDecoder(delimiter,transcoder,10, "MM/DD/YYYY"),
      new VartextStringAsDateDecoder(delimiter,transcoder,10, "MM/DD/YYYY"),
      new VartextStringAsDecimalDecoder(delimiter,transcoder,10,9,2),
      new VartextStringDecoder(delimiter,transcoder,2),
      new VartextStringDecoder(delimiter,transcoder,1),
      new VartextStringAsDateDecoder(delimiter,transcoder,10, "MM/DD/YYYY")
    )
  }
  private def getData(name: String, charset: Charset): ByteBuffer = {
    val example = Resources.toString(Resources.getResource(name), Charsets.UTF_8)
    ByteBuffer.wrap(example.filterNot(_ == '\n').getBytes(charset))
  }

  "vartext" should "decode example with cast" in {
    val buf = getData("vartext0.txt",Ebcdic.charset)
    val decoders = exampleDecoders("|", Ebcdic)
    val cols = decoders.map(_.columnVector(8))

    var i = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)
      readColumn(buf, d, col, 0)
      i += 1
    }

    val dcv = cols(6).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")
  }

  it should "decode bigger example with cast" in {
    val buf = getData("vartext1.txt",Ebcdic.charset)
    val decoders = exampleDecoders("|", Ebcdic)
    val cols = decoders.map(_.columnVector(8))
    ZReader.readBatch(buf, decoders, cols, 8, 74, ByteBuffer.allocate(buf.capacity))

    // reset buffer to replay the data
    buf.clear
    var i = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)
      readColumn(buf, d, col, 0)
      i += 1
    }

    val dcv = cols(6).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")

    val strCol = cols.head.asInstanceOf[BytesColumnVector]
    val strCol2 = cols(7).asInstanceOf[BytesColumnVector]
    var j = 0
    while (j < 8){
      assert(new String(strCol.vector(j),strCol.start(j),strCol.length(j),Charsets.UTF_8) == "US"
        , s"row $j")
      assert(new String(strCol2.vector(j),strCol2.start(j),strCol2.length(j),Charsets.UTF_8) ==
        "WK", s"row $j")
      j += 1
    }
  }

  it should "decode delimited" in {
    val buf = getData("vartext2.txt",Utf8.charset)
    val decoders = exampleDecoders("þ",Utf8)
    val cols = decoders.map(_.columnVector(8))
    ZReader.readBatch(buf, decoders, cols, 8, 74, ByteBuffer.allocate(buf.capacity))

    // reset buffer to replay the data
    buf.clear
    var i = 0
    while (i < decoders.length){
      val d = decoders(i)
      val col = cols(i)
      readColumn(buf, d, col, 0)
      i += 1
    }

    val dcv = cols(6).asInstanceOf[Decimal64ColumnVector]
    assert(dcv.vector(0) == 210L)
    assert(dcv.scale == 2)

    val dateCol = cols.last.asInstanceOf[DateColumnVector]
    val dt = dateCol.formatDate(0)
    assert(dt == "2020-02-07")

    val strCol = cols.head.asInstanceOf[BytesColumnVector]
    val strCol2 = cols(7).asInstanceOf[BytesColumnVector]
    var j = 0
    while (j < 8){
      assert(new String(strCol.vector(j),strCol.start(j),strCol.length(j),Charsets.UTF_8) == "US"
        , s"row $j")
      assert(new String(strCol2.vector(j),strCol2.start(j),strCol2.length(j),Charsets.UTF_8) ==
        "WK", s"row $j")
      j += 1
    }
  }

}
