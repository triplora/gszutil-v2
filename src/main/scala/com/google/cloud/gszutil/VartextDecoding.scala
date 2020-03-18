package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.time.LocalDate

import Decoding._
import com.google.cloud.gzos.pb.Schema.Field
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, DateColumnVector, Decimal64ColumnVector}

object VartextDecoding {
  def getVartextDecoder(f: Field, delimiter: Array[Byte], transcoder: Transcoder): Decoder = {
    import Field.FieldType._
    val filler: Boolean = f.getFiller || f.getName.toUpperCase.startsWith("FILLER")
    if (f.getTyp == STRING) {
      if (f.getCast == INTEGER)
        new VartextStringAsIntDecoder(delimiter, transcoder, f.getSize, filler)
      else if (f.getCast == DATE)
        new VartextStringAsDateDecoder(delimiter, transcoder, f.getSize, f.getFormat, filler)
      else if (f.getCast == DECIMAL)
        new VartextStringAsDecimalDecoder(delimiter, transcoder, f.getSize,
          f.getPrecision, f.getScale, filler)
      else if (f.getCast == LATIN_STRING) {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        new VartextNullableStringDecoder(delimiter, LatinTranscoder, f.getSize, nullIf,
          filler = filler)
      } else {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        if (nullIf.isEmpty)
          new VartextStringDecoder(delimiter, transcoder, f.getSize, filler = filler)
        else
          new VartextNullableStringDecoder(delimiter, transcoder, f.getSize, nullIf, filler =
            filler)
      }
    } else if (f.getTyp == INTEGER) {
      if (f.getCast == DATE){
        new VartextIntegerAsDateDecoder(delimiter, transcoder, f.getSize, f.getFormat, filler)
      } else new VartextStringAsIntDecoder(delimiter, transcoder, f.getSize, filler)
    } else if (f.getTyp == DECIMAL)
      new VartextStringAsDecimalDecoder(delimiter, transcoder, f.getSize, f.getPrecision, f
        .getScale, filler)
    else if (f.getTyp == DATE)
      new VartextStringAsDateDecoder(delimiter, transcoder, f.getSize, f.getFormat, filler)
    else if (f.getTyp == UNSIGNED_INTEGER)
      new VartextStringAsIntDecoder(delimiter, transcoder, f.getSize, filler)
    else
      throw new IllegalArgumentException("unrecognized field type")
  }

  class VartextStringDecoder(override val delimiter: Array[Byte],
                             override val transcoder: Transcoder,
                             override val size: Int,
                             override val filler: Boolean = false)
    extends StringDecoder(transcoder, size, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      super.get(getBuf(buf, delimiter, size),row,i)
    }
  }

  class VartextNullableStringDecoder(override val delimiter: Array[Byte],
                                     override val transcoder: Transcoder,
                                     override val size: Int,
                                     override val nullIf: Array[Byte],
                                     override val filler: Boolean = false)
    extends NullableStringDecoder(transcoder, size, nullIf, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      super.get(getBuf(buf, delimiter, size),row,i)
    }
  }

  class VartextStringAsIntDecoder(override val delimiter: Array[Byte],
                                  override val transcoder: Transcoder,
                                  override val size: Int,
                                  override val filler: Boolean = false)
    extends StringAsIntDecoder(transcoder, size, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      super.get(getBuf(buf, delimiter, size),row,i)
    }
  }

  class VartextStringAsDateDecoder(override val delimiter: Array[Byte],
                                   override val transcoder: Transcoder,
                                   override val size: Int,
                                   override val format: String,
                                   override val filler: Boolean = false)
    extends StringAsDateDecoder(transcoder, size, format, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val str = getStr(buf, transcoder.charset, size)
      val dcv = row.asInstanceOf[DateColumnVector]
      if (str.count(_ == '0') == 8) {
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = LocalDate.from(fmt.parse(str)).toEpochDay
        dcv.vector.update(i, dt)
      }
    }
  }

  class VartextIntegerAsDateDecoder(override val delimiter: Array[Byte],
                                    override val transcoder: Transcoder,
                                    override val size: Int,
                                    override val format: String,
                                    override val filler: Boolean = false)
    extends IntegerAsDateDecoder(size, format, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val long = getStr(buf,transcoder.charset, size).toLong
      putValue(long, row, i)
    }
  }

  class VartextStringAsDecimalDecoder(override val delimiter: Array[Byte],
                                      override val transcoder: Transcoder,
                                      override val size: Int,
                                      precision: Int,
                                      scale: Int,
                                      override val filler: Boolean = false)
    extends StringAsDecimalDecoder(transcoder, size, precision, scale, filler) with VartextDecoder {
    override def get(buf: ByteBuffer, row: ColumnVector, i: Int): Unit = {
      val str = getStr(buf,transcoder.charset, size)
      val j1 = str.indexOf('.')
      val scale1 = str.length - (j1+1)
      require(scale1 == scale, s"$str has scale $scale1 but expected $scale")
      val j0 = str.indexWhere(_ != '0')
      val long =
        if (j0 == -1) 0L
        else (str.substring(j0,j1) + str.substring(j1+1,str.length)).toLong
      row.asInstanceOf[Decimal64ColumnVector].vector.update(i, long)
    }
  }
}
