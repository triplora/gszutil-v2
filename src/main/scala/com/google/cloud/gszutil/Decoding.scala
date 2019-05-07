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
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, StringType, StructField, StructType}


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
    val size: Int
    def apply(src: ByteBuffer): T
  }

  class CharDecoder extends Decoder[String] {
    override val size: Int = 1
    private val decoder = ZReader.CP1047.newDecoder()
    private val cb = CharBuffer.allocate(size)

    override def apply(src: ByteBuffer): String = {
      cb.clear()
      decoder.decode(src, cb, true)
      new String(Array(cb.get(0)))
    }
  }

  case class StringDecoder(n: Int) extends Decoder[String] {
    override val size: Int = n
    private val decoder = ZReader.CP1047.newDecoder()
    private val cb = CharBuffer.allocate(size)

    override def apply(src: ByteBuffer): String = {
      cb.clear()
      decoder.decode(src, cb, true)
      cb.toString
    }
  }

  class IntDecoder2 extends Decoder[Short] {
    override val size: Int = 2
    private val bytes = new Array[Byte](size)

    override def apply(src: ByteBuffer): Short = {
      src.get(bytes)
      ((bytes(0) << 8) |
        (bytes(1) & 255)).toShort
    }
  }

  class IntDecoder4 extends Decoder[Int] {
    override val size: Int = 4
    private val bytes = new Array[Byte](size)

    override def apply(src: ByteBuffer): Int = {
      src.get(bytes)
      (bytes(0) << 24) |
        ((bytes(1) & 255) << 16) |
        ((bytes(2) & 255) << 8) |
        (bytes(3) & 255)
    }
  }

  class LongDecoder8 extends Decoder[Long] {
    override val size: Int = 8
    private val bytes = new Array[Byte](size)

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
    override val size: Int = 6
    private val bytes = new Array[Byte](size)

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

  class NumericDecoder(val precision: Int = 9, val scale: Int = 2) extends Decoder[BigDecimal] {
    override val size: Int = ((precision + scale) / 2) + 1
    private val bytes: Array[Byte] = new Array[Byte](size)

    override def apply(src: ByteBuffer): BigDecimal = {
      src.get(bytes)
      BigDecimal(PackedDecimal.unpack(bytes), scale)
    }
  }

  object CopyBook {
    def apply(s: String): CopyBook = {
      val lines = s.lines.flatMap(parseCopyBookLine).toSeq
      CopyBook(lines)
    }
  }

  case class CopyBook(lines: Seq[CopyBookLine]) {
    def getSchema: StructType =
      StructType(lines.flatMap{
        case CopyBookField(name, pic) =>
          Option(StructField(name, pic.getDataType, nullable = false))
        case _ =>
          None
      })

    def getFieldNames: Seq[String] =
      lines.flatMap{
        case CopyBookField(name, _) => Option(name)
        case _ => None
      }

    def getDecoders: Seq[Decoder[_]] =
      lines.flatMap[Decoder[_],Seq[Decoder[_]]]{
        case CopyBookField(_, pic) => Option(pic.getDecoder)
        case _ => None
      }

    def lrecl: Int = getDecoders.foldLeft(0){(a,b) => a + b.size}

    def reader(): DataSet[Row] = GenericReader(lrecl, getDecoders.toArray)
  }

  case class GenericReader(lrecl: Int, private val decoders: Array[Decoder[_]]) extends DataSet[Row] {
    override val LRECL: Int = lrecl
    override val buf: ByteBuffer = ByteBuffer.allocate(LRECL)

    override def read(buf: ByteBuffer): Row =
      new GenericRow(decoders.map(_(buf)))
  }

  trait DataSet[T] {
    val LRECL: Int
    val buf: ByteBuffer

    def read(array: Array[Byte]): T = {
      buf.clear()
      buf.put(array)
      buf.flip()
      read(buf)
    }

    def read(buf: ByteBuffer): T

    def read(records: Iterator[Array[Byte]]): Iterator[T] = {
      records.takeWhile(_.length == LRECL).map(read)
    }

    def readDD(ddName: String): Iterator[T] =
      read(ZReader.readRecords(ddName))
  }

  sealed trait PIC {
    def getDecoder: Decoder[_]
    def getDataType: DataType
  }
  case object PicInt2 extends PIC {
    override def getDecoder: Decoder[Short] = new IntDecoder2
    override def getDataType: DataType = IntegerType
  }
  case object PicInt4 extends PIC {
    override def getDecoder: Decoder[Int] = new IntDecoder4
    override def getDataType: DataType = IntegerType
  }
  case class PicDecimal(p: Int, s: Int) extends PIC {
    override def getDecoder: Decoder[BigDecimal] = new NumericDecoder(p,s)
    override def getDataType: DataType = DecimalType(p,s)
  }
  case object PicDecimal92 extends PIC {
    override def getDecoder: Decoder[BigDecimal] = new NumericDecoder(9,2)
    override def getDataType: DataType = DecimalType(9,2)
  }
  case object PicChar extends PIC {
    override def getDecoder: Decoder[String] = new CharDecoder
    override def getDataType: DataType = StringType
  }
  case class PicString(size: Int) extends PIC {
    override def getDecoder: Decoder[String] = new StringDecoder(size)
    override def getDataType: DataType = StringType
  }

  val typeMap: Map[String,PIC] = Map(
    "PIC S9(4) COMP." -> PicInt2,
    "PIC S9(4) COMP." -> PicInt2,
    "PIC S9(9) COMP." -> PicInt4,
    "PIC S9(9)V9(2) COMP-3." -> PicDecimal92,
    "PIC X." -> PicChar,
    "PIC X(08)." -> PicString(8),
    "PIC X(16)." -> PicString(16),
    "PIC X(30)." -> PicString(30),
    "PIC X(20)." -> PicString(20),
    "PIC X(2)." -> PicString(20),
    "PIC X(10)." -> PicString(10),
    "PIC S9(03)   COMP-3." -> PicDecimal(9,3),
    "PIC S9(07)   COMP-3." -> PicDecimal(9,7),
    "PIC S9(9)    COMP-3." -> PicDecimal(9,9),
    "PIC S9(9)V99 COMP-3." -> PicDecimal(9,2),
    "PIC S9(6)V99." -> PicDecimal(9,2),
    "PIC S9(13)V99 COMP-3" -> PicDecimal(9,2)
  )

  sealed trait CopyBookLine
  case class CopyBookTitle(name: String) extends CopyBookLine
  case class CopyBookField(name: String, typ: PIC) extends CopyBookLine
  case class Occurs(n: Int) extends CopyBookLine

  val titleRegex = """^(\d{1,2})\s+[A-Z0-9- ]*\.$""".r
  val fieldRegex = """^(\d{1,2})\s+[A-Z0-9- ]*(PIC.*)$""".r
  val occursRegex = """^OCCURS (\d{1,2}) TIMES.$""".r

  def parseCopyBookLine(s: String): Option[CopyBookLine] = {
    val f = s.takeWhile(_ != '*').trim
    f match {
      case fieldRegex(name, typ) =>
        Option(CopyBookField(name, typeMap(typ)))
      case titleRegex(name) =>
        Option(CopyBookTitle(name))
      case occursRegex(n) if n.forall(Character.isDigit) =>
        Option(Occurs(n.toInt))
      case x: String if x.isEmpty => None
      case _ =>
        System.out.println(s"'$f' did not match a regex")
        None
    }
  }
}
