/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.gszutil.io

import com.google.cloud.bigquery.{Field, FieldList, FieldValue, LegacySQLTypeName, StandardSQLTypeName}
import com.google.cloud.gszutil.Transcoder
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.common.io.BaseEncoding
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import java.nio.{ByteBuffer, CharBuffer}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

object AvroUtil {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def printTimestamp(t: java.sql.Timestamp): String =
    formatter.format(t.toInstant.atZone(ZoneId.of("Etc/UTC")))

  def printDate(t: LocalDate): String =
    dateFormatter.format(t)

  def getScale(schema: Schema): Int = schema.getJsonProp("scale").getIntValue

  def readDecimal(buf: ByteBuffer, bytes: Array[Byte], scale: Int): java.math.BigDecimal = {
    System.arraycopy(buf.array(), buf.position(), bytes, 0, 16)
    new java.math.BigDecimal(new java.math.BigInteger(bytes), scale)
  }

  def appendQuotedString(delimiter: Char, s: String, sb: CharBuffer): Unit = {
    if (s.contains(delimiter) || s.contains('\n')) {
      sb.put("\"")
      sb.put(s.replaceAllLiterally("\"", "\\\"").replaceAllLiterally("\n", "\\n"))
      sb.put("\"")
    } else sb.put(s)
  }

  case class StringTranscoder(field: AvroField,
                              transcoder: Transcoder,
                              size: Int) extends AvroTranscoder {
    private val encoder = transcoder.charset.newEncoder()
    private val cb = CharBuffer.allocate(size)

    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val s: String = v.asInstanceOf[org.apache.avro.util.Utf8].toString
      cb.clear()
      cb.put(s)
      cb.flip()
      encoder.encode(cb, buf, true)
    }
  }

  case class LongTranscoder(field: AvroField, size: Int) extends AvroTranscoder {
    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x: Long = v.asInstanceOf[Long]
      com.ibm.dataaccess.ByteArrayMarshaller.writeLong(x, buf.array(), buf.position(), false, size)
      val pos1 = buf.position() + size
      buf.position(pos1)
    }
  }

  case class DecimalTranscoder(field: AvroField, out: Record.Field) extends AvroTranscoder {
    val scale: Int = field.typeSchema.getJsonProp("scale").getIntValue
    @transient private val decimalBuf = new Array[Byte](16)

    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x0: BigDecimal = readDecimal(v.asInstanceOf[ByteBuffer], decimalBuf, scale)
      val x1: Long = x0.toLongExact
      com.ibm.dataaccess.DecimalData.convertLongToPackedDecimal(x1, buf.array(), buf.position(),
        out.getPrecision, true)
      val pos1 = buf.position() + out.getSize
      buf.position(pos1)
    }
  }

  case class BooleanTranscoder(field: AvroField, transcoder: Transcoder) extends AvroTranscoder {
    private val encoder = transcoder.charset.newEncoder()
    private val cb = CharBuffer.allocate(1)

    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x: Boolean = v.asInstanceOf[Boolean]
      cb.clear()
      cb.put(if (x) '1' else '0')
      cb.flip()
      encoder.encode(cb, buf, true)
    }
  }

  case class DateTranscoder(field: AvroField, transcoder: Transcoder) extends AvroTranscoder {
    private val encoder = transcoder.charset.newEncoder()
    private val cb = CharBuffer.allocate(10)

    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x = LocalDate.ofEpochDay(v.asInstanceOf[Int])
      val s = AvroUtil.printDate(x)
      cb.clear()
      cb.put(s)
      cb.flip()
      encoder.encode(cb, buf, true)
    }
  }

  case class TimestampTranscoder(field: AvroField, transcoder: Transcoder) extends AvroTranscoder {
    private val encoder = transcoder.charset.newEncoder()
    private val cb = CharBuffer.allocate(10)

    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x = new java.sql.Timestamp(v.asInstanceOf[Long] / 1000L)
      val s = AvroUtil.printTimestamp(x)
      cb.clear()
      cb.put(s)
      cb.flip()
      encoder.encode(cb, buf, true)
    }
  }

  case class DoubleTranscoder(field: AvroField, out: Record.Field) extends AvroTranscoder {
    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x: Double = v.asInstanceOf[Double]
      val x1 = BigDecimal.valueOf(x).bigDecimal
      com.ibm.dataaccess.DecimalData.convertBigDecimalToPackedDecimal(x1, buf.array(), buf
        .position(), out.getPrecision, true)
      val pos1 = buf.position() + out.getSize
      buf.position(pos1)
    }
  }

  case class FloatTranscoder(field: AvroField, out: Record.Field) extends AvroTranscoder {
    override def read(row: GenericRecord, buf: ByteBuffer): Unit = {
      val v = row.get(field.pos)
      val x: Float = v.asInstanceOf[Float]
      val x1 = BigDecimal.valueOf(x).bigDecimal
      com.ibm.dataaccess.DecimalData.convertBigDecimalToPackedDecimal(x1, buf.array(), buf
        .position(), out.getPrecision, true)
      val pos1 = buf.position() + out.getSize
      buf.position(pos1)
    }
  }

  def transcoder(field: Schema.Field, out: Record.Field, transcoder: Transcoder): AvroTranscoder = {
    val f = AvroField(field)
    if (f.isString) {
      StringTranscoder(f, transcoder, out.getSize)
    } else if (f.isLong) {
      LongTranscoder(f, out.getSize)
    } else if (f.isDecimal) {
      DecimalTranscoder(f, out)
    } else if (f.isDate) {
      DateTranscoder(f, transcoder)
    } else if (f.isTimestamp) {
      TimestampTranscoder(f, transcoder)
    } else if (f.isDouble) {
      DoubleTranscoder(f, out)
    } else if (f.isFloat) {
      FloatTranscoder(f, out)
    } else if (f.isBoolean) {
      BooleanTranscoder(f, transcoder)
    } else {
      throw new RuntimeException(s"Unhandled avro type ${f.typeSchema}")
    }
  }

  def toFieldValue(field: AvroField, value: Any): FieldValue = {
    value match {
      case null => FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)
      case s: org.apache.avro.util.Utf8 if field.isString => FieldValue.of(FieldValue.Attribute.PRIMITIVE, s.toString)
      case s: Long if field.isLong => FieldValue.of(FieldValue.Attribute.PRIMITIVE, s.toString)
      case s: ByteBuffer if field.isDecimal => FieldValue.of(FieldValue.Attribute.PRIMITIVE, handleDecimal(s, field.scale))
      case s: ByteBuffer if field.isBytes => FieldValue.of(FieldValue.Attribute.PRIMITIVE, BaseEncoding.base64.encode(s.array(), s.position(), s.limit()))
      case s: Integer if field.isDate => FieldValue.of(FieldValue.Attribute.PRIMITIVE, dateFormatter.format(LocalDate.ofEpochDay(s.longValue())))
      case _ => throw new IllegalStateException(s"Type [${field.field}] with type [${field.typeSchema.getType}] with value '$value' is not supported!!!")
    }
  }

  private def handleDecimal(s: ByteBuffer, scale: Int): String =
    new java.math.BigDecimal(new java.math.BigInteger(s.array()), scale).doubleValue().toString
}
