/*
 * Copyright 2019 Google LLC All Rights Reserved.
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
import java.nio.charset.{Charset, StandardCharsets}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, Month, ZoneOffset}

import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gzos.pb.Schema.Field
import com.google.cloud.gzos.pb.Schema.Field.NullIf
import com.google.common.base.Charsets
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.TypeDescription


object Decoding extends Logging {
  final val CP1047: Charset = Charset.forName("CP1047")
  final val EBCDIC1: Charset = new EBCDIC1()
  final val LATIN: Charset = StandardCharsets.ISO_8859_1

  // EBCDIC decimal byte values that map to valid ASCII characters
  private final val validAscii: Array[Int] = Array(
    75,76,77,78,79,80,
    91,92,93,94,95,96,97,
    107,108,109,110,111,
    121,122,123,124,125,126,127,
    129,130,131,132,133,134,135,136,137,
    145,146,147,148,149,150,151,152,153,
    161,162,163,164,165,166,167,168,169,
    173,
    189,
    192,193,194,195,196,197,198,199,200,201,
    208,209,210,211,212,213,214,215,216,217,
    224,
    226,227,228,229,230,231,232,233,
    240,241,242,243,244,245,246,247,248,249
  )

  final val Space: Byte = " ".getBytes(Charsets.US_ASCII).head

  // https://en.wikipedia.org/wiki/EBCDIC_1047
  private final val EBCDIC2ASCII: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    val cb = CP1047.decode(buf)
    val a = Array.fill(256)(Space)
    val b = cb.toString.toCharArray.map(_.toByte)
    for (i <- validAscii) a(i) = b(i)

    a(0xBA) = uint(91).toByte // EBCDIC [ is not the same as CP1047
    a(0xBB) = uint(93).toByte // EBCDIC ] is not the same as CP1047
    a
  }

  /** EBCDIC byte to ASCII byte */
  @inline
  final def e2a(b: Byte): Byte = EBCDIC2ASCII(uint(b))

  @inline
  final def e2a(a: Array[Byte]): Array[Byte] = e2a(a,0,a.length)

  @inline
  final def e2a(a: Array[Byte], i0: Int, n: Int): Array[Byte] = {
    var i = i0
    while (i < n){
      a(i) = e2a(a(i))
      i += 1
    }
    a
  }

  @inline
  final def read(buf: ByteBuffer, size: Int, destPos: Int): Array[Byte] =
    read(buf,new Array[Byte](size),size,destPos)

  @inline
  final def read(buf: ByteBuffer, a: Array[Byte], size: Int, destPos: Int): Array[Byte] = {
    val i = buf.position
    val i1 = buf.position + size
    System.arraycopy(buf.array,i,a,destPos,size)
    buf.position(i1)
    a
  }

  def ebcdic2ASCIIBytes(a: Array[Byte]): Array[Byte] = {
    val a1 = new Array[Byte](a.length)
    var i = 0
    while (i < a.length){
      a1(i) = e2a(a(i))
      i += 1
    }
    a1
  }

  def ebcdic2ASCIIString(a: Array[Byte]): String = {
    new String(ebcdic2ASCIIBytes(a), Charsets.UTF_8)
  }

  private final val LATIN1: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    LATIN.decode(buf)
      .toString
      .toCharArray
      .map(_.toByte)
  }

  private final val EBCDIC: Array[Byte] = {
    val buf = ByteBuffer.wrap((0 until 256).map(_.toByte).toArray)
    val a: Array[Byte] = CP1047.decode(buf)
      .toString
      .toCharArray
      .map(_.toByte)
    a(0xBA) = uint(91).toByte // [
    a(0xBB) = uint(93).toByte // ]
    a
  }

  def ebcdic2utf8byte(b: Byte): Byte = EBCDIC(uint(b))

  @inline
  final def uint(b: Byte): Int = if (b < 0) 256 + b else b

  trait Decoder {
    val size: Int
    def filler: Boolean

    /** Read a field into a mutable output builder
      *
      * @param buf ByteBuffer
      * @param row ColumnVector
      * @param i row index
      */
    def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit

    def columnVector(maxSize: Int): ColumnVector

    def typeDescription: TypeDescription

    /** Proto Representation */
    def toFieldBuilder: Field.Builder
  }

  case class LatinStringDecoder(size: Int,
                                nullIf: Array[Byte],
                                filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      val inBuf = buf.array
      val outBuf = bcv.getValPreallocatedBytes

      // copy to output buffer
      val j0 = bcv.getValPreallocatedStart
      val startPos = buf.position
      val endPos = buf.position+size
      System.arraycopy(inBuf,startPos,outBuf,j0,size)
      buf.position(endPos)

      // decode
      var j = j0
      val j1 = j + size
      while (j < j1){
        outBuf.update(j,LATIN1(uint(outBuf(j))))
        j += 1
      }

      // check for null condition
      var k = 0
      var isNull = true
      while (k < nullIf.length){
        if (outBuf(j0+k) != nullIf(k))
          isNull = false
        k += 1
      }
      if (isNull){
        bcv.isNull(i) = true
        bcv.noNulls = false
      }

      // set output
      bcv.setValPreallocated(i, size)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createChar.withMaxLength(size)

    override def toString: String = s"$size byte STRING (LATIN_TO_UNICODE)"

    override def toFieldBuilder: Field.Builder = {
      val b = Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.STRING)

      if (nullIf != null && nullIf.nonEmpty)
        b.setNullif(NullIf.newBuilder
          .setValue(new String(nullIf,Charsets.UTF_8)).build)
      b
    }
  }


  case class NullableStringDecoder(size: Int,
                                   nullIf: Array[Byte],
                                   filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      val inBuf = buf.array
      val outBuf = bcv.getValPreallocatedBytes

      // copy to output buffer
      val j0 = bcv.getValPreallocatedStart
      val startPos = buf.position
      val endPos = buf.position+size
      System.arraycopy(inBuf,startPos,outBuf,j0,size)
      buf.position(endPos)

      // decode
      var j = j0
      val j1 = j + size
      while (j < j1){
        outBuf.update(j,EBCDIC(uint(outBuf(j))))
        j += 1
      }

      // check for null condition
      var k = 0
      var isNull = true
      while (k < nullIf.length){
        if (outBuf(j0+k) != nullIf(k))
          isNull = false
        k += 1
      }
      if (isNull){
        bcv.isNull(i) = true
        bcv.noNulls = false
      }

      // set output
      bcv.setValPreallocated(i, size)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createChar.withMaxLength(size)

    override def toString: String = s"$size byte STRING"

    override def toFieldBuilder: Field.Builder = {
      val b = Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.STRING)

      if (nullIf != null && nullIf.nonEmpty)
        b.setNullif(NullIf.newBuilder
          .setValue(new String(nullIf,Charsets.UTF_8)).build)
      b
    }
  }


  case class StringDecoder(size: Int, filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      val outBuf = bcv.getValPreallocatedBytes
      var j = bcv.getValPreallocatedStart
      val j1 = j + size
      while (j < j1){
        outBuf.update(j,EBCDIC2ASCII(uint(buf.get)))
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

    override def toString: String = s"$size byte STRING NOT NULL"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.STRING)
  }

  case class StringAsIntDecoder(override val size: Int,
                                filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val s = new String(e2a(read(buf,size,0)), Charsets.UTF_8).filter(_.isDigit)
      row.asInstanceOf[LongColumnVector]
        .vector.update(i, s.toLong)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def toString: String = s"$size STRING (INT64)"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.INTEGER)
  }

  trait DateDecoderT extends Decoder {
    val format: String
    protected val pattern: String = format.replaceAllLiterally("D","d").replaceAllLiterally("Y","y")
    protected val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)

    @inline
    protected def toEpochDay(bytes: Array[Byte]): Long =
      toEpochDay(new String(bytes, Charsets.UTF_8))

    @inline
    protected def toEpochDay(s: String): Long =
      LocalDate.from(fmt.parse(s)).toEpochDay

    @inline
    protected def toEpochDay(x: Long): Long =
      LocalDate.from(fmt.parse(x.toString)).toEpochDay
  }

  case class StringAsDateDecoder(override val size: Int,
                                 override val format: String,
                                 filler: Boolean = false) extends DateDecoderT {

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val res = e2a(read(buf,size,0))
      val dcv = row.asInstanceOf[DateColumnVector]
      val count = res.count(_ == 48) // count zeros in string
      if (pattern.length == 10 && count == 8) {
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = toEpochDay(res)
        dcv.vector.update(i, dt)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DateColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDate()

    override def toString: String = s"$size byte STRING (DATE '$format')"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.DATE)
  }

  case class IntegerAsDateDecoder(override val size: Int,
                                  override val format: String,
                                  filler: Boolean = false) extends DateDecoderT {

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val long = Binary.decode(buf,size)
      val dcv = row.asInstanceOf[DateColumnVector]
      if (long <= 0) {
        dcv.noNulls = false
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = toEpochDay(long)
        dcv.vector.update(i, dt)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new DateColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createDate()

    override def toString: String = s"$size byte INT (DATE '$format')"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.DATE)
  }

  case class StringAsDecimalDecoder(override val size: Int,
                                    precision: Int,
                                    scale: Int,
                                    filler: Boolean = false) extends Decoder {
    private def toDecimal(buf: Array[Byte]): Long =
      new String(buf, Charsets.UTF_8)
        .dropWhile(_=='0')
        .filter(c => c.isDigit||c=='-').toLong

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val long = toDecimal(e2a(read(buf,size,0)))
      row.asInstanceOf[Decimal64ColumnVector].vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new Decimal64ColumnVector(maxSize, precision, scale)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(scale)
        .withPrecision(precision)

    override def toString: String = s"$size byte STRING (NUMERIC($precision,$scale))"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.DECIMAL)
        .setPrecision(precision)
        .setScale(scale)
  }


  case class DateDecoder(filler: Boolean = false) extends Decoder {
    override val size: Int = 4
    private final val Time = LocalTime.of(0,0,0)

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[TimestampColumnVector]
      // Dates are stored 4 byte integer offset from 19000000
      val dt = Binary.decode(buf, size).toInt + 19000000
      val year = dt / 10000
      val y = year * 10000
      val month = dt - y
      val day = dt - (y + month*100)
      val localDate = LocalDate.of(year,Month.of(month),day)
      val localDateTime = LocalDateTime.of(localDate,Time)
      val t = localDateTime.toEpochSecond(ZoneOffset.UTC)
      bcv.getScratchTimestamp.setTime(t)
      bcv.setFromScratchTimestamp(i)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector()

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp()

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.DATE)
  }

  case class LongDecoder(override val size: Int,
                         filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      row.asInstanceOf[LongColumnVector]
        .vector.update(i, Binary.decode(buf, size))
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def toString: String = s"$size byte INT64"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.INTEGER)
  }

  case class UnsignedLongDecoder(override val size: Int,
                                 filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      row.asInstanceOf[LongColumnVector]
        .vector.update(i, Binary.decodeUnsigned(buf, size))
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def toString: String = s"$size byte INT64"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.UNSIGNED_INTEGER)
  }

  case class Decimal64Decoder(p: Int, s: Int, filler: Boolean = false) extends Decoder {
    private val precision = p+s
    require(precision <= 18 && precision > 0, s"precision $precision not in range [1,18]")
    override val size: Int = PackedDecimal.sizeOf(p,s)

    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val x = PackedDecimal.unpack(buf, size)
      val vec: Array[Long] = row.asInstanceOf[Decimal64ColumnVector].vector
      if (x > TypeDescription.MAX_DECIMAL64 && PackedDecimal.relaxedParsing) {
        vec.update(i, TypeDescription.MAX_DECIMAL64)
      } else if (x < TypeDescription.MIN_DECIMAL64 && PackedDecimal.relaxedParsing) {
        vec.update(i, TypeDescription.MIN_DECIMAL64)
      } else {
        vec.update(i, x)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new Decimal64ColumnVector(maxSize, precision, s)

    override def typeDescription: TypeDescription =
      TypeDescription.createDecimal
        .withScale(s)
        .withPrecision(p+s)

    override def toString: String = s"$size byte NUMERIC($p,$s)"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setPrecision(precision)
        .setScale(s)
        .setFiller(filler)
        .setTyp(Field.FieldType.DECIMAL)
  }

  def getDecoder(f: Field): Decoder = {
    import Field.FieldType._
    val filler: Boolean = f.getFiller || f.getName.toUpperCase.startsWith("FILLER")
    if (f.getTyp == STRING) {
      if (f.getCast == INTEGER)
        StringAsIntDecoder(f.getSize, filler)
      else if (f.getCast == DATE)
        StringAsDateDecoder(f.getSize, f.getFormat, filler)
      else if (f.getCast == DECIMAL)
        StringAsDecimalDecoder(f.getSize, f.getPrecision, f.getScale, filler)
      else if (f.getCast == LATIN_STRING) {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.getBytes(Charsets.UTF_8))
          .getOrElse(Array.empty)
        LatinStringDecoder(f.getSize, nullIf, filler = filler)
      } else {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.getBytes(Charsets.UTF_8))
          .getOrElse(Array.empty)
        if (nullIf.nonEmpty)
          StringDecoder(f.getSize, filler = filler)
        else
          NullableStringDecoder(f.getSize, nullIf, filler = filler)
      }
    }
    else if (f.getTyp == INTEGER) {
      if (f.getCast == DATE){
        IntegerAsDateDecoder(f.getSize, f.getFormat, filler)
      } else LongDecoder(f.getSize, filler)
    } else if (f.getTyp == DECIMAL)
      Decimal64Decoder(f.getPrecision - f.getScale, f.getScale, filler)
    else if (f.getTyp == DATE)
      DateDecoder(filler)
    else if (f.getTyp == UNSIGNED_INTEGER)
      UnsignedLongDecoder(f.getSize, filler)
    else
      throw new IllegalArgumentException("unrecognized field type")
  }

  private val charRegex = """PIC X\((\d{1,3})\)""".r
  private val numStrRegex = """PIC 9\((\d{1,3})\)""".r
  private val intRegex = """PIC S9\((\d{1,3})\) COMP""".r
  private val uintRegex = """PIC 9\((\d{1,3})\) COMP""".r
  private val decRegex = """PIC S9\((\d{1,3})\) COMP-3""".r
  private val decRegex2 = """PIC S9\((\d{1,3})\)V9\((\d{1,3})\) COMP-3""".r
  private val decRegex3 = """PIC S9\((\d{1,3})\)V(9{1,6}) COMP-3""".r
  def typeMap(typ: String): Decoder = {
    typ.stripSuffix(".") match {
      case charRegex(size) =>
        StringDecoder(size.toInt)
      case "PIC X" =>
        StringDecoder(1)
      case numStrRegex(size) =>
        StringDecoder(size.toInt)
      case decRegex(p) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, 0)
      case decRegex2(p,s) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, s.toInt)
      case decRegex3(p,s) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, s.length)
      case "PIC S9 COMP" =>
        LongDecoder(2)
      case "PIC 9 COMP" =>
        UnsignedLongDecoder(2)
      case intRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
        val x = p.toInt
        if (x <= 4)
          LongDecoder(2)
        else if (x <= 9)
          LongDecoder(4)
        else
          LongDecoder(8)

      case uintRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
        val x = p.toInt
        if (x <= 4)
          UnsignedLongDecoder(2)
        else if (x <= 9)
          UnsignedLongDecoder(4)
        else
          UnsignedLongDecoder(8)
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
  case class CopyBookTitle(name: String) extends CopyBookLine {
    override def toString: String = name
  }
  case class CopyBookField(name: String, decoder: Decoder) extends CopyBookLine {
    override def toString: String = s"${decoder.size}\t$name\t$decoder"
  }
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
