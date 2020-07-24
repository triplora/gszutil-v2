package com.google.cloud.gszutil

import java.time.LocalDate

import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.FieldType

object Encoding {

  def getEncoder(f: Field, transcoder: Transcoder): BinaryEncoder = {
    if (f.getTyp == FieldType.STRING)
      StringToBinaryEncoder(transcoder, f.getSize)
    else if (f.getTyp == FieldType.INTEGER)
      LongToBinaryEncoder(f.getSize)
    else if (f.getTyp == FieldType.DECIMAL)
      DecimalToBinaryEncoder(f.getPrecision, f.getScale)
    else if (f.getTyp == FieldType.DATE)
      DateStringToBinaryEncoder()
    else if (f.getTyp == FieldType.BYTES)
      BytesToBinaryEncoder(f.getSize)
    else throw new UnsupportedOperationException(s"No encoder found for ${f.getTyp}.")
  }

  case class StringToBinaryEncoder(transcoder: Transcoder, size: Int) extends BinaryEncoder {
    def encode(x: String): Array[Byte] = {
      if (x == null)
        return Array.fill(size)(0x00)
      val array = new Array[Byte](size)
      transcoder.charset.encode(x).get(array)
      if (array.length != size)
        throw new RuntimeException(s"encoded length ${array.length} not equal to field size $size")
      array
    }
  }

  case class LongToBinaryEncoder(size: Int) extends BinaryEncoder {
    def encode(x: java.lang.Long): Array[Byte] = {
      if (x == null) Array.fill(size)(0x00)
      else Binary.encode(x, size)
    }
  }

  case class DecimalToBinaryEncoder(p: Int, s: Int) extends BinaryEncoder {
    def encode(x: java.lang.Long): Array[Byte] = {
      val size = PackedDecimal.sizeOf(p, s)
      if (x == null)
        Array.fill(size)(0x00)
      else
        PackedDecimal.pack(x, size)
    }
  }

  case class DateStringToBinaryEncoder() extends BinaryEncoder {
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
    def encode(bytes: Array[Byte]): Array[Byte] = {
      if (bytes == null || bytes.isEmpty)
        Array.fill(size)(0x00)
      else
        bytes
    }
  }
}
