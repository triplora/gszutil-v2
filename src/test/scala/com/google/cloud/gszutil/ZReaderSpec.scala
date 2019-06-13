package com.google.cloud.gszutil

import java.nio.charset.StandardCharsets

import com.google.cloud.gszutil.io._
import com.google.common.hash.Hashing
import org.scalatest.FlatSpec

class ZReaderSpec extends FlatSpec {
  "RecordReader" should "read" in {
    val testBytes = Util.randString(100000).getBytes(StandardCharsets.UTF_8)
    val reader = new ZDataSet(testBytes, 135, 135 * 10)
    val readBytes = Util.readAllBytes(new ZChannel(reader))
    assert(readBytes.length == testBytes.length)
    val matches = Hashing.sha256().hashBytes(testBytes).toString == Hashing.sha256().hashBytes(readBytes).toString
    assert(matches)
  }

  "Decoding" should "transcode EBCDIC" in {
    val test = Util.randString(10000)
    val in = test.getBytes(Decoding.CP1047)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq

    val got = in.map(Decoding.ebcdic2ascii)
    val n = got.length

    assert(n == expected.length)
    assert(got.sameElements(expected))
  }
}
