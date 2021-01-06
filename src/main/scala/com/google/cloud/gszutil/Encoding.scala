package com.google.cloud.gszutil

import java.time.LocalDate

import com.google.cloud.bigquery.{FieldValue, StandardSQLTypeName}
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.FieldType
import com.google.cloud.imf.gzos.{Binary, PackedDecimal}
import com.google.cloud.imf.util.Logging
import com.google.cloud.gszutil.CopyBookDecoderAndEncoderOps._
import com.google.cloud.gszutil.Decoding.{CopyBookField, Decimal64Decoder}

object Encoding extends Logging {

  def getEncoder(f: Field, transcoder: Transcoder): BinaryEncoder = {
    if (f.getTyp == FieldType.STRING)
      StringToBinaryEncoder(transcoder, f.getSize)
    else if (f.getTyp == FieldType.INTEGER)
      LongToBinaryEncoder(f.getSize)
    else if (f.getTyp == FieldType.DECIMAL)
      DecimalToBinaryEncoder(f.getPrecision - f.getScale, f.getScale)
    else if (f.getTyp == FieldType.DATE)
      DateStringToBinaryEncoder()
    else if (f.getTyp == FieldType.BYTES)
      BytesToBinaryEncoder(f.getSize)
    else
      UnknownTypeEncoder
  }

  def getEncoder(cbf: CopyBookField, transcoder: Transcoder): BinaryEncoder = {
    val decoderSize = cbf.decoder.size
    val typ = cbf.fieldType
    typ.stripSuffix(".") match {
      case charRegex(_) =>
        val name = cbf.name.toUpperCase
        if( (name.endsWith("DT") || name.endsWith("DATE")) && decoderSize == 10) {
          DateStringToBinaryEncoder()
        } else {
          StringToBinaryEncoder(transcoder, decoderSize)
        }
      case "PIC X" | numStrRegex(_) =>
        StringToBinaryEncoder(transcoder, decoderSize)
      case decRegex(p) if p.toInt >= 1 && cbf.decoder.isInstanceOf[Decimal64Decoder] =>
        val dec = cbf.decoder.asInstanceOf[Decimal64Decoder]
        DecimalToBinaryEncoder(dec.p, dec.s)
      case decRegex2(p,_) if p.toInt >= 1 && cbf.decoder.isInstanceOf[Decimal64Decoder]=>
        val dec = cbf.decoder.asInstanceOf[Decimal64Decoder]
        DecimalToBinaryEncoder(dec.p, dec.s)
      case decRegex3(p,_) if p.toInt >= 1 && cbf.decoder.isInstanceOf[Decimal64Decoder]=>
        val dec = cbf.decoder.asInstanceOf[Decimal64Decoder]
        DecimalToBinaryEncoder(dec.p, dec.s)
      case "PIC S9 COMP" | "PIC 9 COMP"  =>
        LongToBinaryEncoder(decoderSize)
      case intRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
        LongToBinaryEncoder(decoderSize)
      case uintRegex(p) if p.toInt <= 18 && p.toInt >= 1 =>
          LongToBinaryEncoder(decoderSize)
      case x if types.contains(x)=>
        types(x)._2
      case _ =>
        UnknownTypeEncoder
    }
  }

  case class StringToBinaryEncoder(transcoder: Transcoder, size: Int) extends BinaryEncoder {
    override type T = String
    override val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.STRING
    override def encode(x: String): Array[Byte] = {
      if (x == null)
        return Array.fill(size)(0x00)

      if (x.length > size) {
        val msg = s"ERROR StringToBinaryEncoder string overflow ${x.length} > $size"
        logger.error(msg)
        throw new RuntimeException(msg)
      }

      val diff = size - x.length
      val toEncode = if (diff > 0)
        String.format(s"%-${size}s", x)
      else x

      val buf = transcoder.charset.encode(toEncode)
      if (buf.remaining() != size)
        throw new RuntimeException(s"String length mismatch: ${buf.remaining()} != $size")
      val array = new Array[Byte](size)
      buf.get(array)
      array
    }

    override def encodeValue(value: FieldValue): Array[Byte] = {
      value.getValue match {
        case s: String =>
          encode(s)
        case x =>
          if (x == null) encode(null)
          else throw new UnsupportedOperationException(s"Unsupported field value ${x.getClass.getSimpleName}")
      }
    }
  }

  case class LongToBinaryEncoder(size: Int) extends BinaryEncoder {
    override type T = java.lang.Long
    override val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.INT64
    override def encode(x: T): Array[Byte] = {
      if (x == null) Array.fill(size)(0x00)
      else Binary.encode(x, size)
    }

    override def encodeValue(value: FieldValue): Array[Byte] = {
      if (value.isNull) Array.fill(size)(0x00)
      else {
        value.getValue match {
          case s: String =>
            encode(s.toLong)
          case x =>
            throw new RuntimeException(s"Invalid long: $x")
        }
      }
    }
  }

  case class DecimalToBinaryEncoder(p: Int, s: Int) extends BinaryEncoder {
    override type T = java.lang.Long
    override val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.NUMERIC
    override val size: Int = PackedDecimal.sizeOf(p, s)
    override def encode(x: T): Array[Byte] = {
      if (x == null)
        Array.fill(size)(0x00)
      else
        PackedDecimal.pack(x, size)
    }

    override def encodeValue(value: FieldValue): Array[Byte] = {
      if (value.isNull) Array.fill(size)(0x00)
      else {
        value.getValue match {
          case s0: String =>
            var v1 = s0.toDouble
            var scale = 0
            while (scale < s) {
              v1 *= 10d
              scale += 1
            }
            encode(v1.toLong)
          case x =>
            throw new RuntimeException(s"Invalid decimal: $x")
        }
      }
    }
  }

  case class DateStringToBinaryEncoder() extends BinaryEncoder {
    override type T = String
    override val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.DATE
    override val size = 4
    override def encode(x: String): Array[Byte] = {
      if (x == null)
        Array.fill(size)(0x00)
      else {
        val date = LocalDate.parse(x)
        val int = ((((date.getYear - 1900) * 100) +
          date.getMonthValue) * 100) +
          date.getDayOfMonth
        Binary.encode(int, size)
      }
    }

    override def encodeValue(value: FieldValue): Array[Byte] =
      if (value.isNull) Array.fill(size)(0x00)
      else throw new UnsupportedOperationException()
  }

  case class BytesToBinaryEncoder(size: Int) extends BinaryEncoder {
    override type T = Array[Byte]
    override val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.BYTES

    def encode(bytes: Array[Byte]): Array[Byte] = {
      if (bytes == null || bytes.isEmpty)
        Array.fill(size)(0x00)
      else {
        if (bytes.length != size)
          throw new RuntimeException(s"Size mismatch: byte array length ${bytes.length} != $size")
        bytes
      }
    }

    override def encodeValue(value: FieldValue): Array[Byte] =
      if (value.isNull) encode(null)
      else encode(value.getBytesValue)
  }

  case object UnknownTypeEncoder extends BinaryEncoder {
    override type T = Object
    override def size = 0
    override val bqSupportedType: StandardSQLTypeName = null
    override def encode(elem: Object): Array[Byte] =
      throw new UnsupportedOperationException()
    override def encodeValue(value: FieldValue): Array[Byte] =
      throw new UnsupportedOperationException()
  }
}
