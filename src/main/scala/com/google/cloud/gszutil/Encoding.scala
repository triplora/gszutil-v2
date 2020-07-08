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
    else throw new UnsupportedOperationException(s"No encoder found for ${f.getTyp}.")
  }

  case class StringToBinaryEncoder(transcoder: Transcoder, size: Int) extends BinaryEncoder {
    def encode(x: String): Array[Byte] = {
      val array = new Array[Byte](size)
      transcoder.charset.encode(x).get(array)
      array
    }
  }

  case class LongToBinaryEncoder(size: Int) extends BinaryEncoder {
    def encode(x: Long): Array[Byte] = {
      Binary.encode(x, size)
    }
  }

  case class DecimalToBinaryEncoder(p: Int, s: Int) extends BinaryEncoder {
    def encode(x: Long): Array[Byte] = {
      PackedDecimal.pack(x, PackedDecimal.sizeOf(p,s))
    }
  }

  case class DateStringToBinaryEncoder() extends BinaryEncoder {
    val size = 4

    def encode(x: String): Array[Byte] = {
      val date = LocalDate.parse(x)
      val int = ((((date.getYear - 1900) * 100) +
        date.getMonthValue) * 100) +
        date.getDayOfMonth

      Binary.encode(int, size)
    }
  }

}
