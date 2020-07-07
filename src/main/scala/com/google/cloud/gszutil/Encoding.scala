package com.google.cloud.gszutil

import java.nio.ByteBuffer
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
    def encode(x: String): ByteBuffer = {
      transcoder.charset.encode(x)
    }
  }

  case class LongToBinaryEncoder(size: Int) extends BinaryEncoder {
    def encode(x: Long): ByteBuffer = {
      ByteBuffer.allocate(size).put(Binary.encode(x, size))
    }
  }

  case class DecimalToBinaryEncoder(p: Int, s: Int) extends BinaryEncoder {
    def encode(x: Long): ByteBuffer = {
      val size = PackedDecimal.sizeOf(p,s)
      ByteBuffer.allocate(size).put(PackedDecimal.pack(x, size))
    }
  }

  case class DateStringToBinaryEncoder() extends BinaryEncoder {
    val size = 4

    def encode(x: String): ByteBuffer = {
      val date = LocalDate.parse(x)
      ByteBuffer.allocate(size)
    }
  }

}
