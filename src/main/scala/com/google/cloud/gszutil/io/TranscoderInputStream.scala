package com.google.cloud.gszutil.io

import java.io.InputStream
import java.nio.charset.{Charset, CharsetDecoder, CharsetEncoder, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}


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
  private val lrecl: Int = reader.lRecl // maximum record length
  private val data: Array[Byte] = new Array[Byte](lrecl)
  private val bufSz: Int = size
  private val in: ByteBuffer = ByteBuffer.allocate(bufSz)
  private val out: ByteBuffer = ByteBuffer.allocate(bufSz)
  out.position(out.capacity)
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
  override def read(): Int = throw new UnsupportedOperationException()
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
