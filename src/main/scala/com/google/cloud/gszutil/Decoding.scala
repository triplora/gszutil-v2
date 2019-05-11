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

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.google.cloud.gszutil.io.ZReader
import com.google.common.base.Charsets
import com.ibm.jzos.fields.{BinaryAsIntField, BinaryAsLongField}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer


object Decoding {
  final val CP1047: Charset = Charset.forName("CP1047")

  final val EBCDIC: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    val cb = CP1047.decode(buf)
    cb.toString.toCharArray.map(_.toByte)
  }

  def ebdic2ascii(b: Byte): Byte = {
    EBCDIC(uint(b))
  }

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
    val dataType: DataType
    def get(a: Array[Byte], off: Int): T

    /** Read a field into a mutable output builder
      *
      * @param a byte array
      * @param off offset within a to begin reading
      * @param row mutable output builder
      * @param i field index
      */
    def get(a: Array[Byte], off: Int, row: SpecificInternalRow, i: Int): Unit =
      row.update(i, get(a, off))
  }

  case class StringDecoder(override val size: Int) extends Decoder[String] {
    override val dataType: DataType = StringType
    private val buf = new Array[Byte](size)
    override def get(a: Array[Byte], off: Int): String = {
      var i = 0
      while (i < size){
        buf(i) = ebdic2ascii(a(off + i))
        i+=1
      }
      new String(buf, Charsets.UTF_8)
    }

    override def get(a: Array[Byte], off: Int, row: SpecificInternalRow, i: Int): Unit = {
      val str = get(a, off)
      val utf8 = UTF8String.fromString(str)
      row.update(i, utf8)
    }
  }

  case class IntDecoder(override val size: Int) extends Decoder[Int] {
    override val dataType: DataType = IntegerType
    private lazy val field = new BinaryAsIntField(0, size, true)
    override def get(a: Array[Byte], off: Int): Int =
      field.getInt(a, off)
  }

  case class LongDecoder(override val size: Int) extends Decoder[Long] {
    override val dataType: DataType = LongType
    private lazy val field = new BinaryAsLongField(0, size, true)
    override def get(a: Array[Byte], off: Int): Long =
      field.getLong(a, off)
  }

  case class DecimalDecoder(precision: Int, scale: Int) extends Decoder[BigDecimal] {
    override val dataType: DataType = DecimalType(precision, scale)
    override val size: Int = ((precision + scale) / 2) + 1
    override def get(a: Array[Byte], off: Int): BigDecimal =
      BigDecimal(PackedDecimal.unpack(a, off, size), scale)
    override def get(a: Array[Byte], off: Int, row: SpecificInternalRow, i: Int): Unit =
      row.update(i, new Decimal().set(get(a, off)))
  }

  case class CopyBook(raw: String) {
    lazy val lines = raw.lines.flatMap(parseCopyBookLine).toSeq

    def getSchema: StructType =
      StructType(lines.flatMap{
        case CopyBookField(name, pic) =>
          val safeName = name.replaceAllLiterally("-","_")
          Option(StructField(safeName, pic.getDataType, nullable = false))
        case _ =>
          None
      })

    def getFieldNames: Seq[String] =
      lines.flatMap{
        case CopyBookField(name, _) => Option(name)
        case _ => None
      }

    def getDecoders: Seq[Decoder[_]] = {
      val buf = ArrayBuffer.empty[Decoder[_]]
      lines.foreach{
        case CopyBookField(_, pic) =>
          val decoder = pic.getDecoder
          buf.append(decoder)
        case _ =>
      }
      buf.result.toArray.toSeq
    }

    def lRecl: Int = getDecoders.foldLeft(0){_ + _.size}

    def reader: ZReader = new ZReader(this)
  }

  sealed trait PIC {
    def getDecoder: Decoder[_]
    def getDataType: DataType
  }
  case class PicInt(size: Int) extends PIC {
    override def getDecoder: Decoder[Int] = IntDecoder(size)
    override def getDataType: DataType = IntegerType
  }
  case class PicDecimal(p: Int, s: Int) extends PIC {
    override def getDecoder: Decoder[BigDecimal] = DecimalDecoder(p, s)
    override def getDataType: DataType = DecimalType(p,s)
  }
  case class PicString(size: Int) extends PIC {
    override def getDecoder: Decoder[String] = StringDecoder(size)
    override def getDataType: DataType = StringType
  }

  val typeMap: Map[String,PIC] = Map(
    "PIC S9(4) COMP." -> PicInt(2),
    "PIC S9(9) COMP." -> PicInt(4),
    "PIC S9(9)V9(2) COMP-3." -> PicDecimal(9,2),
    "PIC X." -> PicString(1),
    "PIC X(8)." -> PicString(8),
    "PIC X(16)." -> PicString(16),
    "PIC X(30)." -> PicString(30),
    "PIC X(20)." -> PicString(20),
    "PIC X(2)." -> PicString(20),
    "PIC X(10)." -> PicString(10),
    "PIC S9(3) COMP-3." -> PicDecimal(9,3),
    "PIC S9(7) COMP-3." -> PicDecimal(9,7),
    "PIC S9(9) COMP-3." -> PicDecimal(9,9),
    "PIC S9(9)V99 COMP-3." -> PicDecimal(9,2),
    "PIC S9(6)V99." -> PicDecimal(9,2),
    "PIC S9(13)V99 COMP-3" -> PicDecimal(9,2)
  )

  sealed trait CopyBookLine
  case class CopyBookTitle(name: String) extends CopyBookLine
  case class CopyBookField(name: String, typ: PIC) extends CopyBookLine
  case class Occurs(n: Int) extends CopyBookLine

  private val titleRegex = """^\d{1,2}\s+([A-Z0-9-]*)\.$""".r
  private val fieldRegex = """^\d{1,2}\s+([A-Z0-9-]*)\s*(PIC.*)$""".r
  private val occursRegex = """^OCCURS (\d{1,2}) TIMES.$""".r

  def parseCopyBookLine(s: String): Option[CopyBookLine] = {
    val f = s.takeWhile(_ != '*').trim
    f match {
      case fieldRegex(name, typ) =>
        val typ1 = typ
          .replaceFirst("""\s+COMP""", " COMP")
          .replaceFirst("""\(0""", """\(""")
        Option(CopyBookField(name.trim, typeMap(typ1)))
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
