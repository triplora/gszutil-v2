/*
 * Copyright 2019 Google LLC
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

import java.io.InputStream
import java.nio.charset._
import java.nio.{ByteBuffer, CharBuffer}

import com.ibm.jzos.{RecordReader, ZFile}

object ZReader {
  trait TRecordReader {
    def read(buf: Array[Byte]): Int
    def read(buf: Array[Byte], off: Int, len: Int): Int
    def close(): Unit
    def getLrecl: Int
  }

  class WrappedRecordReader(r: RecordReader) extends TRecordReader {
    override def read(buf: Array[Byte]): Int =
      r.read(buf)
    override def read(buf: Array[Byte], off: Int, len: Int): Int =
      r.read(buf, off, len)
    override def close(): Unit = r.close()
    override def getLrecl: Int = r.getLrecl
  }

  def readDD(ddName: String): InputStream = {
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    val reader = RecordReader.newReaderForDD(ddName)
    reader.setAutoFree(true)
    System.out.println(s"Reading DD $ddName ${reader.getDsn} with record format ${reader.getRecfm} BLKSIZE ${reader.getBlksize} LRECL ${reader.getLrecl}")
    new RecordReaderInputStream(new WrappedRecordReader(reader), 65536)
  }

  final val EBCDIC: Charset = Charset.forName("IBM1047")

  private def newDecoder(): CharsetDecoder = EBCDIC.newDecoder
    .onMalformedInput(CodingErrorAction.REPORT)
    .onUnmappableCharacter(CodingErrorAction.REPORT)

  private def newEncoder(): CharsetEncoder = StandardCharsets.UTF_8.newEncoder
    .onMalformedInput(CodingErrorAction.REPORT)
    .onUnmappableCharacter(CodingErrorAction.REPORT)

  class RecordReaderInputStream(reader: TRecordReader, size: Int) extends InputStream {
    private val decoder = newDecoder()
    private val encoder = newEncoder()
    private val lrecl = reader.getLrecl // maximum record length
    private val data = new Array[Byte](lrecl)
    private val bufSz = size
    private val in = ByteBuffer.allocate(bufSz)
    private val out = {
      val b = ByteBuffer.allocate(bufSz)
      b.flip()
      b
    }
    private val cb = CharBuffer.allocate(bufSz)
    private var endOfInput = false
    private var endOfOutput = false
    private var bytesIn: Long = 0
    private var bytesOut: Long = 0

    def fill(): Unit = {
      if (!endOfInput) {
        out.compact()
        var n = 0
        while (in.remaining > lrecl && n > -1) {
          n = reader.read(data)
          if (n < 1) {
            endOfInput = true
          } else {
            bytesIn += n
            in.put(data, 0, n)
          }
        }

        in.flip()
        decoder.decode(in, cb, endOfInput)
        cb.flip()
        encoder.encode(cb, out, endOfInput)
        in.compact()
        cb.compact()
        out.flip()
      }
    }

    def getBytesIn: Long = bytesIn
    def getBytesOut: Long = bytesOut

    override def read(): Int = throw new NotImplementedError()

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (out.remaining < len) fill()

      if (!endOfOutput) {
        val nBytes = math.min(out.remaining, len)
        if (nBytes < len) endOfOutput = true
        bytesOut += nBytes
        out.get(b, off, nBytes)
        nBytes
      } else -1
    }

    override def close(): Unit = {
      System.out.println(s"Read $bytesIn bytes from RecordReader and output $bytesOut bytes")
      super.close()
    }
  }
}
