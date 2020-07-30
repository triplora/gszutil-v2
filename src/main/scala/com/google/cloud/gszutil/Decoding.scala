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
import java.time.{LocalDate, Month}

import com.google.cloud.imf.gzos.{Binary, PackedDecimal}
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.NullIf
import com.google.cloud.imf.util.Logging
import com.google.protobuf.ByteString
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DateColumnVector, Decimal64ColumnVector, LongColumnVector, TimestampColumnVector}
import org.apache.orc.TypeDescription


object Decoding extends Logging {
  class NullableStringDecoder(transcoder: Transcoder,
                              override val size: Int,
                              override val nullIf: Array[Byte],
                              override val filler: Boolean = false) extends NullableDecoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val bcv = col.asInstanceOf[BytesColumnVector]

      // decode into output buffer
      bcv.ensureValPreallocated(size)
      val destPos = bcv.getValPreallocatedStart
      val dest = bcv.getValPreallocatedBytes
      transcoder.arraycopy(buf, dest, destPos, size)

      // set output
      if (isNull(dest, destPos, nullIf)){
        bcv.isNull(i) = true
        bcv.noNulls = false
        bcv.setValPreallocated(i, 0)
      } else {
        bcv.setValPreallocated(i, size)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size*2)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createString()

    override def toString: String = s"$size byte STRING"

    override def toFieldBuilder: Field.Builder = {
      val b = Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.STRING)

      if (nullIf != null && nullIf.nonEmpty)
        b.setNullif(NullIf.newBuilder
          .setValue(ByteString.copyFrom(nullIf)).build)
      b
    }
  }

  class StringDecoder(transcoder: Transcoder,
                      override val size: Int,
                      override val filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val bcv = col.asInstanceOf[BytesColumnVector]
      bcv.ensureValPreallocated(size)
      transcoder.arraycopy(buf, bcv.getValPreallocatedBytes, bcv.getValPreallocatedStart, size)
      bcv.setValPreallocated(i, size)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size*2)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createString()

    override def toString: String = s"$size byte STRING NOT NULL"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.STRING)
  }

  class StringAsIntDecoder(transcoder: Transcoder,
                           override val size: Int,
                           override val filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val long = transcoder.getLong(buf,size)
      col.asInstanceOf[LongColumnVector].vector.update(i, long)
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new LongColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createLong

    override def toString: String = s"$size byte STRING (INT64)"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.INTEGER)
  }

  class StringAsDateDecoder(transcoder: Transcoder,
                            override val size: Int,
                            override val format: String,
                            override val filler: Boolean = false) extends DateDecoder {

    private val Zero: Byte = "0".getBytes(transcoder.charset).head
    // count zeros to detect null
    protected def isNull(buf: ByteBuffer): Boolean = {
      var zeros = 0
      var j = buf.position()
      val j1 = math.min(j+size, buf.limit())
      val a = buf.array
      while (j < j1) {
        if (a(j) == Zero) zeros += 1
        j += 1
      }
      zeros == 8
    }

    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val dcv = col.asInstanceOf[DateColumnVector]
      if (isNull(buf)) {
        val i1 = buf.position() + size
        buf.position(i1)
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = transcoder.getEpochDay(buf,size,fmt)
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

  class StringAsTimestampDecoder(transcoder: Transcoder,
                                 override val size: Int,
                                 override val filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val dcv = col.asInstanceOf[TimestampColumnVector]
      val s = transcoder.getString(buf,size)
      if (s == "0000-00-00 00:00:00.000000") {
        val i1 = buf.position() + size
        buf.position(i1)
        dcv.isNull.update(i, true)
        dcv.noNulls = false
      } else {
        val ts = java.sql.Timestamp.valueOf(s)
        dcv.time.update(i, ts.getTime)
        dcv.nanos.update(i, ts.getNanos)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector =
      new TimestampColumnVector(maxSize)

    override def typeDescription: TypeDescription =
      TypeDescription.createTimestamp()

    override def toString: String = s"$size byte STRING (TIMESTAMP)"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder()
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.TIMESTAMP)
  }

  class IntegerAsDateDecoder(override val size: Int,
                             override val format: String = "YYYYMMDD",
                             override val filler: Boolean = false) extends DateDecoder {

    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val long = Binary.decode(buf,size)
      putValue(long, col, i)
    }

    protected def putValue(long: Long, row: ColumnVector, i: Int): Unit = {
      val dcv = row.asInstanceOf[DateColumnVector]
      if (long <= 0) {
        dcv.noNulls = false
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = LocalDate.from(fmt.parse(long.toString)).toEpochDay
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

  class StringAsDecimalDecoder(transcoder: Transcoder,
                               override val size: Int,
                               val precision: Int,
                               val scale: Int,
                               override val filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val long = transcoder.getLong(buf,size)
      col.asInstanceOf[Decimal64ColumnVector].vector.update(i, long)
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

  /** Decode date from 4 byte binary integer offset from 19000000 */
  case class IntAsDateDecoder(filler: Boolean = false) extends Decoder {
    override val size: Int = 4

    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val dcv = col.asInstanceOf[DateColumnVector]
      val long = Binary.decode(buf, size)
      if (long == 0){
        dcv.noNulls = false
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = (long + 19000000).toInt
        val year = dt / 10000
        val y = year * 10000
        val month = (dt - y)/100
        val day = dt - (y + month*100)
        val localDate = LocalDate.of(year,Month.of(month),day)
        dcv.vector.update(i, localDate.toEpochDay)
      }
    }

    override def columnVector(maxSize: Int): ColumnVector = new DateColumnVector

    override def typeDescription: TypeDescription = TypeDescription.createDate

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.DATE)
  }

  case class LongDecoder(override val size: Int,
                         filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      col.asInstanceOf[LongColumnVector]
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
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      col.asInstanceOf[LongColumnVector]
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

    def isNull(buf: Array[Byte], pos: Int): Boolean = {
      var i = pos
      val limit = pos + size
      while (i < limit){
        if (buf(i) != 0x00) return false
        i += 1
      }
      true
    }

    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val dcv = col.asInstanceOf[Decimal64ColumnVector]
      if (isNull(buf.array(), buf.position())){
        dcv.noNulls = false
        dcv.isNull.update(i, true)
      } else {
        val x = PackedDecimal.unpack(buf, size)
        dcv.vector.update(i, x)
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

  /** Simply copies bytes from input directly to column vector */
  class BytesDecoder(override val size: Int,
                     override val filler: Boolean = false) extends Decoder {
    override def get(buf: ByteBuffer, col: ColumnVector, i: Int): Unit = {
      val bcv = col.asInstanceOf[BytesColumnVector]
      bcv.ensureValPreallocated(size)
      val startPos = buf.position()
      val newPos = startPos + size
      System.arraycopy(buf.array(), startPos, bcv.getValPreallocatedBytes,
        bcv.getValPreallocatedStart, size)
      buf.position(newPos)
      bcv.setValPreallocated(i, size)
    }

    override def columnVector(maxSize: Int): ColumnVector = {
      val cv = new BytesColumnVector(maxSize)
      cv.initBuffer(size)
      cv
    }

    override def typeDescription: TypeDescription =
      TypeDescription.createBinary()

    override def toString: String = s"$size BYTES"

    override def toFieldBuilder: Field.Builder =
      Field.newBuilder
        .setSize(size)
        .setFiller(filler)
        .setTyp(Field.FieldType.BYTES)
  }

  def getDecoder(f: Field, transcoder: Transcoder): Decoder = {
    import Field.FieldType._
    val filler: Boolean = f.getFiller || f.getName.toUpperCase.startsWith("FILLER")
    if (f.getTyp == STRING) {
      if (f.getCast == INTEGER)
        new StringAsIntDecoder(transcoder, f.getSize, filler)
      else if (f.getCast == DATE)
        new StringAsDateDecoder(transcoder, f.getSize, f.getFormat, filler)
      else if (f.getCast == TIMESTAMP || f.getCast == DATETIME)
        new StringAsTimestampDecoder(transcoder, f.getSize, filler)
      else if (f.getCast == DECIMAL)
        new StringAsDecimalDecoder(transcoder, f.getSize, f.getPrecision, f.getScale, filler)
      else if (f.getCast == BYTES)
        new BytesDecoder(f.getSize, filler)
      else if (f.getCast == LATIN_STRING) {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        new NullableStringDecoder(LatinTranscoder, f.getSize, nullIf, filler = filler)
      } else {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        if (nullIf.isEmpty)
          new StringDecoder(transcoder, f.getSize, filler = filler)
        else
          new NullableStringDecoder(transcoder, f.getSize, nullIf, filler = filler)
      }
    }
    else if (f.getTyp == INTEGER) {
      if (f.getCast == DATE){
        new IntegerAsDateDecoder(f.getSize, filler = filler)
      } else LongDecoder(f.getSize, filler)
    } else if (f.getTyp == DECIMAL)
      Decimal64Decoder(f.getPrecision - f.getScale, f.getScale, filler)
    else if (f.getTyp == DATE)
      IntAsDateDecoder(filler)
    else if (f.getTyp == UNSIGNED_INTEGER)
      UnsignedLongDecoder(f.getSize, filler)
    else if (f.getTyp == BYTES)
      new BytesDecoder(f.getSize, filler)
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
  def typeMap(typ: String, transcoder: Transcoder, filler: Boolean): Decoder = {
    typ.stripSuffix(".") match {
      case charRegex(size) =>
        new StringDecoder(transcoder, size.toInt, filler = filler)
      case "PIC X" =>
        new StringDecoder(transcoder, 1, filler = filler)
      case numStrRegex(size) =>
        new StringDecoder(transcoder, size.toInt, filler = filler)
      case decRegex(p) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, 0, filler = filler)
      case decRegex2(p,s) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, s.toInt, filler = filler)
      case decRegex3(p,s) if p.toInt >= 1 =>
        Decimal64Decoder(p.toInt, s.length, filler = filler)
      case "PIC S9 COMP" =>
        LongDecoder(2, filler = filler)
      case "PIC 9 COMP" =>
        UnsignedLongDecoder(2, filler = filler)
      case intRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
        val x = p.toInt
        if (x <= 4)
          LongDecoder(2, filler = filler)
        else if (x <= 9)
          LongDecoder(4, filler = filler)
        else
          LongDecoder(8, filler = filler)

      case uintRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
        val x = p.toInt
        if (x <= 4)
          UnsignedLongDecoder(2, filler = filler)
        else if (x <= 9)
          UnsignedLongDecoder(4, filler = filler)
        else
          UnsignedLongDecoder(8, filler = filler)
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

  def parseCopyBookLine(s: String, transcoder: Transcoder): Option[CopyBookLine] = {
    val f = s.takeWhile(_ != '*').trim
    f match {
      case fieldRegex(name, typ) =>
        val typ1 = typ
          .replaceFirst("""\s+COMP""", " COMP")
          .replaceFirst("""\(0""", """\(""")
        val filler = name.toUpperCase.startsWith("FILLER")
        val decoder = typeMap(typ1, transcoder, filler)

        Option(CopyBookField(name.replace('-','_').trim, decoder))
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
