package com.google.cloud.gszutil.io

import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}

/** Iterator for text MVS Data Set
  *
  * @param reader TRecordReader
  * @param srcCharset input Charset (typically CP1047)
  * @param trim whether to trim trailing whitespace from each record
  */
class RecordIterator(reader: TRecordReader, srcCharset: Charset, trim: Boolean) extends Iterator[String] {
  private val data: Array[Byte] = new Array[Byte](reader.lRecl)
  private val in: ByteBuffer = ByteBuffer.allocate(reader.lRecl)
  private val cb: CharBuffer = CharBuffer.allocate(reader.lRecl)
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
