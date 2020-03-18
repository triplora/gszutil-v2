package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.Charset

trait VartextDecoder extends Decoder {
  def delimiter: Array[Byte]
  def transcoder: Transcoder

  /** (len,newpos) */
  def nextDelimiterOrNewLine(delim: Array[Byte], buf: ByteBuffer, newline: Array[Byte]): (Int,Int)
  = {
    val i = buf.array.indexOfSlice(delimiter, buf.position)
    val j = buf.array.indexOfSlice(newline, buf.position)
    val k = buf.limit
    val i1 = if (j >= 0) math.min(j, k) else k
    if (i == -1){
      (i1 - buf.position, i1)
    } else {
      if (i <= i1)
        (i - buf.position, i+delim.length)
      else
        (i1 - buf.position, i1)
    }
  }

  /** (len,newpos) */
  def nextDelimiter(delim: Array[Byte], buf: ByteBuffer, maxSize: Int): (Int,Int) = {
    val i = buf.array.indexOfSlice(delimiter, buf.position)
    val j = buf.position + maxSize
    if (i == -1){
      val i1 = math.min(j, buf.limit)
      (i1 - buf.position, i1)
    } else {
      if (i <= j)
        (i - buf.position, i+delim.length)
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

  /** Return a read-only buffer containing current field */
  protected final def getBuf(buf: ByteBuffer, delimiter: Array[Byte], maxSize: Int): ByteBuffer = {
    val i = math.min(buf.array.indexOfSlice(delimiter, buf.position), buf.position + maxSize)
    if (i < 0){
      val buf1 = buf.duplicate
      val i1 = buf.position + maxSize
      buf1.position(buf.position)
      buf1.limit(i1)
      buf.position(i1)
      buf1
    } else {
      val buf1 = buf.duplicate
      buf1.position(buf.position)
      buf1.limit(i)
      buf.position(i + delimiter.length)
      buf1
    }
  }
}
