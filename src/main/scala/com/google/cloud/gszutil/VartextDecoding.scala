package com.google.cloud.gszutil

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.LocalDate

import Decoding._
import com.google.cloud.gzos.pb.Schema.Field
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DateColumnVector, Decimal64ColumnVector, LongColumnVector}

object VartextDecoding {
  def getVartextDecoder(f: Field, transcoder: Transcoder): Decoder = {
    import Field.FieldType._
    val filler: Boolean = f.getFiller || f.getName.toUpperCase.startsWith("FILLER")
    if (f.getTyp == STRING) {
      if (f.getCast == INTEGER)
        new VartextStringAsIntDecoder(transcoder, f.getSize, filler)
      else if (f.getCast == DATE)
        new VartextStringAsDateDecoder(transcoder, f.getSize, f.getFormat, filler)
      else if (f.getCast == DECIMAL)
        new VartextStringAsDecimalDecoder(transcoder, f.getSize,
          f.getPrecision, f.getScale, filler)
      else if (f.getCast == LATIN_STRING) {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        new VartextNullableStringDecoder(LatinTranscoder, f.getSize, nullIf,
          filler = filler)
      } else {
        val nullIf = Option(f.getNullif)
          .map(_.getValue.toByteArray)
          .getOrElse(Array.empty)
        if (nullIf.isEmpty)
          new VartextStringDecoder(transcoder, f.getSize, filler = filler)
        else
          new VartextNullableStringDecoder(transcoder, f.getSize, nullIf, filler =
            filler)
      }
    } else if (f.getTyp == INTEGER) {
      if (f.getCast == DATE){
        new VartextIntegerAsDateDecoder(transcoder, f.getSize, f.getFormat, filler)
      } else new VartextStringAsIntDecoder(transcoder, f.getSize, filler)
    } else if (f.getTyp == DECIMAL)
      new VartextStringAsDecimalDecoder(transcoder, f.getSize, f.getPrecision, f
        .getScale, filler)
    else if (f.getTyp == DATE)
      new VartextStringAsDateDecoder(transcoder, f.getSize, f.getFormat, filler)
    else if (f.getTyp == UNSIGNED_INTEGER)
      new VartextStringAsIntDecoder(transcoder, f.getSize, filler)
    else
      throw new IllegalArgumentException("unrecognized field type")
  }

  class VartextStringDecoder(override val transcoder: Transcoder,
                             override val size: Int,
                             override val filler: Boolean = false)
    extends StringDecoder(transcoder, size, filler) with VartextDecoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      bcv.setRef(i, s.getBytes(StandardCharsets.UTF_8), 0, s.length)
    }
  }

  class VartextNullableStringDecoder(override val transcoder: Transcoder,
                                     override val size: Int,
                                     override val nullIf: Array[Byte],
                                     override val filler: Boolean = false)
    extends NullableStringDecoder(transcoder, size, nullIf, filler) with VartextDecoder {
    private val nullStr = new String(nullIf, transcoder.charset)
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val bcv = row.asInstanceOf[BytesColumnVector]
      if (s == nullStr){
        bcv.isNull(i) = true
        bcv.noNulls = false
        bcv.setValPreallocated(i, 0)
      } else {
        bcv.setRef(i, s.getBytes(StandardCharsets.UTF_8), 0, s.length)
      }
    }
  }

  class VartextStringAsIntDecoder(override val transcoder: Transcoder,
                                  override val size: Int,
                                  override val filler: Boolean = false)
    extends StringAsIntDecoder(transcoder, size, filler) with VartextDecoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      row.asInstanceOf[LongColumnVector].vector.update(i, s.toLong)
    }
  }

  class VartextStringAsDateDecoder(override val transcoder: Transcoder,
                                   override val size: Int,
                                   override val format: String,
                                   override val filler: Boolean = false)
    extends StringAsDateDecoder(transcoder, size, format, filler) with VartextDecoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val dcv = row.asInstanceOf[DateColumnVector]
      if (s.count(_ == '0') == 8) {
        dcv.vector.update(i, -1)
        dcv.isNull.update(i, true)
      } else {
        val dt = LocalDate.from(fmt.parse(s)).toEpochDay
        dcv.vector.update(i, dt)
      }
    }
  }

  class VartextIntegerAsDateDecoder(override val transcoder: Transcoder,
                                    override val size: Int,
                                    override val format: String,
                                    override val filler: Boolean = false)
    extends IntegerAsDateDecoder(size, format, filler) with VartextDecoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      putValue(s.toLong, row, i)
    }
  }

  class VartextStringAsDecimalDecoder(override val transcoder: Transcoder,
                                      override val size: Int,
                                      precision: Int,
                                      scale: Int,
                                      override val filler: Boolean = false)
    extends StringAsDecimalDecoder(transcoder, size, precision, scale, filler) with VartextDecoder {
    override def get(s: String, row: ColumnVector, i: Int): Unit = {
      val j1 = s.indexOf('.')
      val scale1 = s.length - (j1+1)
      require(scale1 == scale, s"$s has scale $scale1 but expected $scale")
      val j0 = s.indexWhere(_ != '0')
      val long =
        if (j0 == -1) 0L
        else (s.substring(j0,j1) + s.substring(j1+1,s.length)).toLong
      row.asInstanceOf[Decimal64ColumnVector].vector.update(i, long)
    }
  }
}
