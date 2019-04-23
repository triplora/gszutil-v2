package com.google.cloud.gszutil

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.google.cloud.gszutil.ZReader.{RecordIterator, TranscoderInputStream}
import org.scalatest.FlatSpec

import scala.collection.mutable.ArrayBuffer

class ZReaderSpec extends FlatSpec {
  def readAllBytes(is: InputStream): Array[Byte] = {
    val b = ArrayBuffer.empty[Byte]
    val buf = new Array[Byte](1024)
    var n = 0
    while (n > -1) {
      n = is.read(buf)
      if (n == buf.length)
        b ++= buf
      else if (n > 0)
        b ++= buf.slice(0,n)
    }
    val r = b.result().toArray
    r
  }

  "ZReader" should "transcode EBCDIC" in {
    val test = (0 until 65536).map{x => s"test $x\nABCD\tXYZ\n1234"}.mkString("\n")
    val in = test.getBytes(ZReader.CP1047)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq

    val is = new TranscoderInputStream(
      reader = new TestRecordReader(in, 135),
      size = 65536,
      srcCharset = ZReader.CP1047,
      destCharset = ZReader.UTF8)
    val got = readAllBytes(is).toSeq
    val n = got.length
    assert(is.getBytesIn == expected.length)
    assert(is.getBytesOut == expected.length)
    assert(n == expected.length)
    assert(got == expected)
  }
}
