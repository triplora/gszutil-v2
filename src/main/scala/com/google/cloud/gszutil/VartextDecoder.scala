package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.Charset

trait VartextDecoder extends Decoder {
  def delimiter: Byte
  def transcoder: Transcoder

  /** (len,newpos) */
  def nextDelimiter(delim: Byte, buf: ByteBuffer, maxSize: Int): (Int,Int) = {
    val i = buf.array.indexOf(delimiter, buf.position)
    val j = buf.position + maxSize
    if (i == -1){
      val i1 = math.min(j, buf.limit)
      (i1 - buf.position, i1)
    } else {
      if (i <= j)
        (i - buf.position, i+1)
      else
        (j - buf.position, j)
    }
  }

  protected def getStr(buf: ByteBuffer, charset: Charset, maxSize: Int): String = {
    val (len,newPos) = nextDelimiter(delimiter, buf, maxSize)
    val s = new String(buf.array, buf.position, len, charset)
    buf.position(newPos)
    s
  }
}
