/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.google.cloud.gszutil.Util.Logging
import com.google.common.base.Charsets
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, Decimal64ColumnVector, DecimalColumnVector, LongColumnVector}
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.functionConverterImpls.AsJavaSupplier


object Decoding extends Logging {
  final val CP1047: Charset = Charset.forName("CP1047")

  final val EBCDIC: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    val cb = CP1047.decode(buf)
    cb.toString.toCharArray.map(_.toByte)
  }

  def ebcdic2ascii(b: Byte): Byte = EBCDIC(uint(b))

  def ebcdic2utf8(a: Array[Byte]): String = {
    new String(ebcdic2ascii(a), Charsets.UTF_8)
  }

  def ebcdic2ascii(a: Array[Byte]): Array[Byte] = {
    val a1 = new Array[Byte](a.length)
    var i = 0
    while (i < a.length){
      a1(i) = ebcdic2ascii(a(i))
      i += 1
    }
    a1
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

  trait Decoder {
    val size: Int

    /** Read a field into a mutable output builder
      *
      * @param buf ByteBuffer
      * @param i field index
      */
    def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit

    def columnVector(maxSize: Int): ColumnVector

    def typeDescription: TypeDescription
  }

  case class StringDecoder(override val size: Int) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      val res = bcv.getValPreallocatedBytes
      var j = bcv.getValPreallocatedStart
      val j1 = j + size
      while (j < j1){
        res(j) = EBCDIC(uint(buf.get))
        j += 1
      }
      bcv.setValPreallocated(i, size)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createChar().withMaxLength(size)
  }

  case class LongDecoder(override val size: Int) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val v = PackedDecimal.unpack(buf, size)
      row.asInstanceOf[LongColumnVector]
        .vector.update(i, v)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong
  }

  /** The maximum length of a computational item is 18 decimal digits,
    * except for a PACKED-DECIMAL item. If the ARITH(COMPAT) compiler option
    * is in effect, then the maximum length of a PACKED-DECIMAL item is
    * 18 decimal digits. If the ARITH(EXTEND) compiler option is in effect,
    * then the maximum length of a PACKED-DECIMAL item is 31 decimal digits.
    *
    * @param p numeric character positions
    * @param s decimal scaling positions
    */
  case class DecimalDecoder(p: Int, s: Int) extends Decoder {
    require(p + s <= 31 && p + s > 18, "precision must be in range [19,31]")
    override val size: Int = PackedDecimal.sizeOf(p,s)

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val r = row.asInstanceOf[DecimalColumnVector]
      var remaining = size
      var remainingPrecision = p + s
      val b = ArrayBuffer.empty[String]
      val sub = PackedDecimal.sizeOf(18-2,0)
      while (remaining > sub) {
        val u = PackedDecimal.unpack2(buf, sub)
        val s = f"$u%018d"
        b.append(s)
        remaining -= sub
        remainingPrecision -= 18
      }
      val u1 = PackedDecimal.unpack(buf, remaining)
      val s1 = u1.toString

      if (s1.length < remainingPrecision) {
        val s1a = new String(Array.fill[Char](remainingPrecision - s1.length)('0'))
        b.append(s1a)
      }
      b.append(s1)
      val s2 = b.result().toArray
        .map(a => a.toString).mkString("")
        .getBytes(Charsets.UTF_8)

      val w = new HiveDecimalWritable()
      w.setFromDigitsOnlyBytesWithScale(false, s2, 0, s2.length, 2)
      r.set(i, w)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DecimalColumnVector(maxSize, p+s, s)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(s)
        .withPrecision(p+s)
  }

  case class Decimal64Decoder(p: Int, s: Int) extends Decoder {
    require(p+s <= 18 && p+s > 0, "precision must be in range [1,18]")
    override val size: Int = PackedDecimal.sizeOf(p,s)

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val r = row.asInstanceOf[Decimal64ColumnVector]
      val x = PackedDecimal.unpack(buf, size)
      val w = r.getScratchWritable
      w.setFromLongAndScale(x, s)
      r.set(i, w)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new Decimal64ColumnVector(maxSize, p+s, s)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(s)
        .withPrecision(p+s)
  }

  private val charRegex = """PIC X\((\d{1,3})\)""".r
  private val intRegex = """PIC S9\((\d{1,3})\) COMP""".r
  private val decRegex = """PIC S9\((\d{1,3})\) COMP-3""".r
  private val decRegex2 = """PIC S9\((\d{1,3})\)V9\((\d{1,3})\) COMP-3""".r
  private val decRegex3 = """PIC S9\((\d{1,3})\)V(9{1,6}) COMP-3""".r
  def typeMap(typ: String): Decoder = {
    typ.stripSuffix(".") match {
      case charRegex(size) =>
        StringDecoder(size.toInt)
      case "PIC X" =>
        StringDecoder(1)
      case decRegex(scale) =>
        Decimal64Decoder(9, scale.toInt)
      case decRegex2(p,s) =>
        p.toInt match {
          case x if x < 18 =>
            Decimal64Decoder(p.toInt, s.toInt)
          case _ =>
            DecimalDecoder(p.toInt, s.toInt)
        }
      case decRegex3(p,s) =>
        p.toInt match {
          case x if x < 18 =>
            Decimal64Decoder(p.toInt, s.length)
          case _ =>
            DecimalDecoder(p.toInt, s.length)
        }
      case "PIC S9 COMP" =>
        LongDecoder(2)
      case intRegex(p) if p.toInt <= 18 =>
        val x = p.toInt
        if (x <= 4)
          LongDecoder(2)
        else if (x <= 9)
          LongDecoder(4)
        else
          LongDecoder(8)
      case x =>
        types(x)
    }
  }

  val types: Map[String,Decoder] = Map(
    "PIC S9(6)V99 COMP-3" -> Decimal64Decoder(9,2),
    "PIC S9(13)V99 COMP-3" -> Decimal64Decoder(9,2),
    "PIC S9(7)V99 COMP-3" -> Decimal64Decoder(7,2)
  )

  sealed trait CopyBookLine
  case class CopyBookTitle(name: String) extends CopyBookLine
  case class CopyBookField(name: String, decoder: Decoder) extends CopyBookLine
  case class Occurs(n: Int) extends CopyBookLine

  private val titleRegex = """^\d{1,2}\s+([A-Z0-9-_]*)\.$""".r
  private val titleRegex2 = """^[A-Z]+\s+\d{1,2}\s+([A-Z0-9-_]*)\.$""".r
  private val fieldRegex = """^\d{1,2}\s+([A-Z0-9-_]*)\s*(PIC.*)$""".r
  private val occursRegex = """^OCCURS (\d{1,2}) TIMES.$""".r

  def parseCopyBookLine(s: String): Option[CopyBookLine] = {
    val f = s.takeWhile(_ != '*').trim
    f match {
      case fieldRegex(name, typ) =>
        val typ1 = typ
          .replaceFirst("""\s+COMP""", " COMP")
          .replaceFirst("""\(0""", """\(""")
        val decoder = typeMap(typ1)
        logger.debug(s"parsed $s as $decoder")
        Option(CopyBookField(name.trim, decoder))
      case titleRegex(name) =>
        Option(CopyBookTitle(name))
      case titleRegex2(name) =>
        Option(CopyBookTitle(name))
      case occursRegex(n) if n.forall(Character.isDigit) =>
        Option(Occurs(n.toInt))
      case x: String if x.isEmpty =>
        None
      case _ =>
        throw new RuntimeException(s"'$f' did not match a regex")
    }
  }
}
