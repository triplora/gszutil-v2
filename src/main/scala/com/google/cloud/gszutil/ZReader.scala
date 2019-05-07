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
import java.nio.channels.ReadableByteChannel
import java.nio.charset._
import java.nio.{ByteBuffer, CharBuffer}


object ZReader {
  final val CP1047: Charset = Charset.forName("CP1047")
  final val UTF8: Charset = StandardCharsets.UTF_8


  trait TRecordReader {

    /** Read a record from the dataset into a buffer.
      *
      * @param buf - the byte array into which the bytes will be read
      * @return the number of bytes read, -1 if EOF encountered.
      */
    def read(buf: Array[Byte]): Int

    /** Read a record from the dataset into a buffer.
      *
      * @param buf the byte array into which the bytes will be read
      * @param off the offset, inclusive in buf to start reading bytes
      * @param len the number of bytes to read
      * @return the number of bytes read, -1 if EOF encountered.
      */
    def read(buf: Array[Byte], off: Int, len: Int): Int

    /** Close the reader and underlying native file.
      * This will also free the associated DD if getAutoFree() is true.
      * Must be issued by the same thread as the factory method.
      */
    def close(): Unit

    /** LRECL is the maximum record length for variable length files.
      *
      * @return
      */
    def getLrecl: Int

    /** Maximum block size
      *
      * @return
      */
    def getBlksize: Int
  }

  class InputStreamCloser(is: InputStream) extends Thread {
    override def run(): Unit = {
      is.close()
    }
  }

  def readDD(ddName: String): InputStream = {
    val rr = ZOS.readDD(ddName)
    val is = new TranscoderInputStream(rr, rr.getBlksize, srcCharset = CP1047, destCharset = UTF8)
    Runtime.getRuntime.addShutdownHook(new InputStreamCloser(is))
    is
  }

  def readRecords(ddName: String): Iterator[Array[Byte]] =
    ByteIterator(ZOS.readDD(ddName))

  class RecordReaderChannel(reader: TRecordReader) extends ReadableByteChannel {
    private var hasRemaining = true
    private var open = true
    private val data: Array[Byte] = new Array[Byte](reader.getBlksize)
    private val buf: ByteBuffer = ByteBuffer.wrap(data)
    private var bytesRead: Long = 0
    buf.position(buf.capacity)

    override def read(dst: ByteBuffer): Int = {
      if (buf.remaining < dst.capacity){
        buf.compact()
        val n = reader.read(data, buf.position, buf.remaining)
        if (n > 0) {
          buf.position(buf.position + n)
          bytesRead += n
        } else if (n < 0){
          reader.close()
          hasRemaining = false
        }
        buf.flip()
      }
      val n = math.min(buf.remaining, dst.remaining)
      dst.put(data, buf.position, n)
      buf.position(buf.position + n)
      if (hasRemaining || n > 0) {
        n
      } else {
        close()
        -1
      }
    }

    override def isOpen: Boolean = open

    override def close(): Unit = open = false

    def getBytesRead: Long = bytesRead
  }

  /** Iterator for Binary MVS Data Set
    *
    * @param reader
    */
  case class ByteIterator(private val reader: TRecordReader) extends Iterator[Array[Byte]] {
    private val rc = new RecordReaderChannel(reader)
    private val buf: ByteBuffer = ByteBuffer.allocate(reader.getLrecl)

    def getBytesIn: Long = rc.getBytesRead

    override def hasNext: Boolean = rc.isOpen

    override def next(): Array[Byte] = {
      buf.clear()
      if (rc.read(buf) == reader.getLrecl) {
        buf.array()
      } else null
    }
  }

  /** Iterator for text MVS Data Set
    *
    * @param reader TRecordReader
    * @param srcCharset input Charset (typically CP1047)
    * @param trim whether to trim trailing whitespace from each record
    */
  class RecordIterator(reader: TRecordReader, srcCharset: Charset, trim: Boolean) extends Iterator[String] {
    private val data: Array[Byte] = new Array[Byte](reader.getLrecl)
    private val in: ByteBuffer = ByteBuffer.allocate(reader.getLrecl)
    private val cb: CharBuffer = CharBuffer.allocate(reader.getLrecl)
    private var endOfInput = fill()
    private var bytesIn: Long = 0
    private var bytesWaiting: Int = 0

    private def fill(): Boolean = {
      bytesWaiting = reader.read(data)
      if (bytesWaiting < 1) {
        true
      } else {
        bytesIn += bytesWaiting
        false
      }
    }

    private val decoder: CharsetDecoder = srcCharset.newDecoder
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    def getBytesIn: Long = bytesIn

    override def hasNext: Boolean = !endOfInput

    override def next(): String = {
      if (endOfInput)
        throw new NoSuchElementException("next on empty iterator")

      in.put(data, 0, bytesWaiting)
      in.flip()
      decoder.decode(in, cb, endOfInput)
      cb.flip()
      in.clear()

      if (trim) {
        var i = cb.limit - 1
        while (i >= cb.position && Character.isWhitespace(cb.get(i))) {
          i -= 1
        }
        cb.limit(i+1)
      }
      val result = cb.toString
      cb.clear()
      endOfInput = fill()
      result
    }
  }

  /** Transcodes records
    *
    * @param reader input RecordReader
    * @param size buffer size
    * @param srcCharset input Charset (typically CP1047)
    * @param destCharset output Charset (typically UTF8)
    */
  class TranscoderInputStream(reader: TRecordReader, size: Int, srcCharset: Charset, destCharset: Charset) extends InputStream {
    private val decoder: CharsetDecoder = srcCharset.newDecoder
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    private val encoder: CharsetEncoder = destCharset.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    private val lrecl: Int = reader.getLrecl // maximum record length
    private val data: Array[Byte] = new Array[Byte](lrecl)
    private val bufSz: Int = size
    private val in: ByteBuffer = ByteBuffer.allocate(bufSz)
    private val out: ByteBuffer = {
      val b = ByteBuffer.allocate(bufSz)
      b.flip()
      b
    }
    private val cb: CharBuffer = CharBuffer.allocate(bufSz)
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
      reader.close()
      super.close()
    }
  }
}
