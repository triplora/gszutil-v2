package com.google.cloud.gszutil

import java.time.LocalDate

import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.FieldType
import com.google.cloud.imf.gzos.{Binary, PackedDecimal}

object Encoding {

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

  case class StringToBinaryEncoder(transcoder: Transcoder, size: Int) extends BinaryEncoder {
    val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.STRING
    def encode(x: String): Array[Byte] = {
      if (x == null)
        return Array.fill(size)(0x00)

      if (x.length > size) {
        val msg = s"Length of string is higher than field size. String len=${x.length}, field size: $size."
        throw new RuntimeException(msg)
      }

      val diff = size - x.length
      val toEncode = if (diff > 0)
        String.format(s"%${size}s", x)
      else x

      val buf = transcoder.charset.encode(toEncode)
      val array = new Array[Byte](size)
      buf.get(array)

      if (array.length != size)
        throw new RuntimeException(s"Encoded length ${array.length} not equal to field size $size.")
      array
    }
  }

  case class LongToBinaryEncoder(size: Int) extends BinaryEncoder {
    val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.INT64
    def encode(x: java.lang.Long): Array[Byte] = {
      if (x == null) Array.fill(size)(0x00)
      else Binary.encode(x, size)
    }
  }

  case class DecimalToBinaryEncoder(p: Int, s: Int) extends BinaryEncoder {
    val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.FLOAT64
    val size: Int = PackedDecimal.sizeOf(p, s)
    def encode(x: java.lang.Long): Array[Byte] = {
      if (x == null)
        Array.fill(size)(0x00)
      else
        PackedDecimal.pack(x, size)
    }
  }

  case class DateStringToBinaryEncoder() extends BinaryEncoder {
    val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.DATE
    val size = 4
    def encode(x: String): Array[Byte] = {
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

  }

  case class BytesToBinaryEncoder(size: Int) extends BinaryEncoder {
    val bqSupportedType: StandardSQLTypeName = StandardSQLTypeName.BYTES

    def encode(bytes: Array[Byte]): Array[Byte] = {
      if (bytes == null || bytes.isEmpty)
        Array.fill(size)(0x00)
      else
        bytes
    }
  }

  case object UnknownTypeEncoder extends BinaryEncoder {
    def size = 0
    val bqSupportedType: StandardSQLTypeName = null
  }
}
