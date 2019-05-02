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

import java.nio.{ByteBuffer, CharBuffer}

import org.apache.spark.sql.Row


object Decoding {
  def uint(b: Byte): Int = {
    if (b < 0) 256 + b
    else b
  }

  def pad(x: Int): String = {
    val s = x.toString
    val n = s.length
    if (n < 4)
      "    ".substring(0, 4 - n) + s
    else s
  }

  trait Decoder[T] {
    def apply(src: ByteBuffer): T
  }

  class CharDecoder extends Decoder[String] {
    private val decoder = ZReader.CP1047.newDecoder()
    private val cb = CharBuffer.allocate(1)

    override def apply(src: ByteBuffer): String = {
      cb.clear()
      decoder.decode(src, cb, true)
      new String(Array(cb.get(0)))
    }
  }

  class IntDecoder2 extends Decoder[Short] {
    private val bytes = new Array[Byte](2)

    override def apply(src: ByteBuffer): Short = {
      src.get(bytes)
      ((bytes(0) << 8) |
        (bytes(1) & 255)).toShort
    }
  }

  class IntDecoder4 extends Decoder[Int] {
    private val bytes = new Array[Byte](4)

    override def apply(src: ByteBuffer): Int = {
      src.get(bytes)
      (bytes(0) << 24) |
        ((bytes(1) & 255) << 16) |
        ((bytes(2) & 255) << 8) |
        (bytes(3) & 255)
    }
  }

  class LongDecoder8 extends Decoder[Long] {
    private val bytes = new Array[Byte](8)

    override def apply(src: ByteBuffer): Long = {
      src.get(bytes)
      ((bytes(0) & 255L) << 56) |
        ((bytes(1) & 255L) << 48) |
        ((bytes(2) & 255L) << 40) |
        ((bytes(3) & 255L) << 32) |
        ((bytes(4) & 255L) << 24) |
        ((bytes(5) & 255L) << 16) |
        ((bytes(6) & 255L) << 8) |
        (bytes(7) & 255L)
    }
  }

  class LongDecoder6 extends Decoder[Long] {
    private val bytes = new Array[Byte](6)

    override def apply(src: ByteBuffer): Long = {
      src.get(bytes)
      ((bytes(0) & 255L) << 40) |
        ((bytes(1) & 255L) << 32) |
        ((bytes(2) & 255L) << 24) |
        ((bytes(3) & 255L) << 16) |
        ((bytes(4) & 255L) << 8) |
        (bytes(5) & 255L)
    }
  }

  class NumericDecoder_9_2 extends Decoder[BigDecimal] {
    private val decoder = new LongDecoder6

    override def apply(src: ByteBuffer): BigDecimal = {
      BigDecimal(decoder(src), 2)
    }
  }

  trait DataSet[T] {
    val LRECL: Int

    def read(array: Array[Byte]): T

    def read(buf: ByteBuffer): T

    def read(records: Iterator[Array[Byte]]): Iterator[T]

    def readDD(ddName: String): Iterator[T]
  }

  def decoderName(cpyType: String): String = {
    typeMap.getOrElse(cpyType, "UNKNOWN")
  }

  val typeMap: Map[String,String] = Map(
    "PIC S9(4) COMP." -> "IntDecoder2",
    "PIC S9(9) COMP." -> "IntDecoder4",
    "PIC S9(9)V9(2) COMP-3." -> "NumericDecoder_9_2",
    "PIC X." -> "CharDecoder"
  )

  sealed trait CopyBookLine
  case class CopyBookTitle(name: String) extends CopyBookLine
  case class CopyBookField(name: String, typ: String, Decoder: String) extends CopyBookLine
  case class Occurs(n: Int) extends CopyBookLine

  def parseCopyBook(s: String): Seq[CopyBookLine] = {
    s.lines.flatMap(parseCopyBookLine).toSeq
  }

  val titleRegex = """^(\d{1,2})\s+[A-Z0-9- ]*\.$""".r
  val fieldRegex = """^(\d{1,2})\s+[A-Z0-9- ]*(PIC.*)$""".r
  val occursRegex = """^OCCURS (\d{1,2}) TIMES.$""".r
  def parseCopyBookLine(s: String): Option[CopyBookLine] = {
    val f = s.takeWhile(_ != '*').trim
    f match {
      case fieldRegex(name,typ) =>
        Option(CopyBookField(name, typ, decoderName(typ)))
      case titleRegex(name) =>
        Option(CopyBookTitle(name))
      case occursRegex(n) if n.forall(Character.isDigit) =>
        Option(Occurs(n.toInt))
      case _ =>
        System.out.println(s"'$f' did not match a regex")
        None
    }
  }
}
