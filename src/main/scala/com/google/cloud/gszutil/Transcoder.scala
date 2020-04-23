package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.LocalDate
import java.time.format.DateTimeFormatter

trait Transcoder {
  val charset: Charset

  @inline
  protected final def uint(b: Byte): Int = if (b < 0) 256 + b else b

  protected final lazy val bytes: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    charset.decode(buf)
      .toString
      .toCharArray
      .map(_.toByte)
  }

  /** Read String from EBCDIC in ByteBuffer */
  @inline
  final def getString(buf: ByteBuffer, size: Int): String = {
    val i1 = buf.position() + size
    val s = new String(buf.array, buf.position(), size, charset)
    buf.position(i1)
    s
  }

  /** Read EBCDIC in ByteBuffer */
  @inline
  final def arraycopy(buf: ByteBuffer, dest: Array[Byte], destPos: Int, size: Int): Array[Byte] = {
    val i1 = buf.position() + size
    System.arraycopy(buf.array, buf.position(), dest, destPos, size)
    buf.position(i1)
    var i = destPos
    while (i < destPos + size){
      dest.update(i, decodeByte(dest(i)))
      i += 1
    }
    dest
  }

  /** Read Long from EBCDIC in ByteBuffer */
  @inline
  final def getLong(buf: ByteBuffer, size: Int): Long = {
    val i1 = buf.position() + size
    val s = new String(buf.array, buf.position(), size, charset).filter(c => c.isDigit || c == '-')
    buf.position(i1)
    s.toLong
  }

  /** Read Epoch Day from EBCDIC in ByteBuffer */
  @inline
  def getEpochDay(buf: ByteBuffer, size: Int, fmt: DateTimeFormatter): Long = {
    val i1 = buf.position() + size
    val s = new String(buf.array, buf.position(), size, charset)
    val s1 = s.filter(_.isDigit)
    buf.position(i1)
    LocalDate.from(fmt.parse(s1)).toEpochDay
  }

  /** Convert byte from EBCDIC to ASCII */
  @inline
  final def decodeByte(b: Byte): Byte = bytes(uint(b))

  /** Convert byte array from EBCDIC to ASCII in place */
  @inline
  final def decodeBytes(a: Array[Byte]): Array[Byte] = {
    var i = 0
    while (i < a.length){
      a.update(i, decodeByte(a(i)))
      i += 1
    }
    a
  }
}
