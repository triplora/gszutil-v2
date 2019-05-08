package com.google.cloud.gszutil

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets

import com.google.cloud.gszutil.ZReader.{ByteIterator, RecordReaderChannel, TranscoderInputStream, ZIterator}
import com.google.cloud.gszutil.io.ByteArrayRecordReader
import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding
import org.scalatest.FlatSpec
import org.zeromq.codec.Z85

import scala.util.Random

class ZReaderSpec extends FlatSpec {
  def transfer(in: ReadableByteChannel, out: WritableByteChannel) = {
    val buf = ByteBuffer.allocate(4096)
    while (in.read(buf) >= 0){
      buf.flip()
      out.write(buf)
      buf.compact()
    }
  }

  def readAllBytes(is: InputStream): Array[Byte] =
    readAllBytes(Channels.newChannel(is))

  def readAllBytes(in: ReadableByteChannel): Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val out = Channels.newChannel(os)
    transfer(in, out)
    os.toByteArray
  }

  def randBytes(len: Int): Array[Byte] = {
    val bytes = new Array[Byte](len)
    Random.nextBytes(bytes)
    bytes
  }

  def randString(len: Int): String =
    Z85.Z85Encoder(randBytes(len))

  "RecordReader" should "read" in {
    val testBytes = randString(100000).getBytes(StandardCharsets.UTF_8)
    val reader = new ByteArrayRecordReader(testBytes, 135, 135 * 10)
    val readBytes = readAllBytes(new RecordReaderChannel(reader))
    assert(readBytes.length == testBytes.length)
    val matches = Hashing.sha256().hashBytes(testBytes).toString == Hashing.sha256().hashBytes(readBytes).toString
    assert(matches)
  }

  "ZIterator" should "read" in {
    val lrecl = 135
    val blkSize = lrecl*10
    val testBytes = randString(blkSize*2).getBytes(StandardCharsets.UTF_8)//.slice(0, blkSize*4)

    def read() = new ZIterator(new ByteArrayRecordReader(testBytes, lrecl, blkSize))

    val records = read()
    records.zip(testBytes.grouped(lrecl)).foreach{x =>
      val l = x._1._1.slice(x._1._2, x._1._2 + lrecl)
      val r = x._2
      assert(l.sameElements(r))
    }
  }

  "ZReader" should "transcode EBCDIC" in {
    val test = randString(1000000)
    val in = test.getBytes(ZReader.CP1047)
    val expected = test.getBytes(StandardCharsets.UTF_8).toSeq

    val is = new TranscoderInputStream(
      reader = new ByteArrayRecordReader(in, 135, 135 * 10),
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
