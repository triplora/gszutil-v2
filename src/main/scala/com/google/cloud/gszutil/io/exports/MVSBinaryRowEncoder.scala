package com.google.cloud.gszutil.io.exports

import com.google.cloud.bigquery._
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.Encoding._
import com.google.cloud.gszutil.{BinaryEncoder, Transcoder}
import com.google.cloud.imf.util.CloudLogging

import java.nio.ByteBuffer

object MVSBinaryRowEncoder {

  def toBinary(row: FieldValueList,
               mvsEncoders: Array[BinaryEncoder],
               dest: ByteBuffer): Result = {
    val rowSize = row.size()
    var index = 0
    while (index < rowSize) {
      val fieldValue: FieldValue = row.get(index)
      if (fieldValue.getAttribute != FieldValue.Attribute.PRIMITIVE) {
        val msg = s"Field type is not PRIMITIVE. fieldId=$index"
        return Result.Failure(msg)
      }

      val encoder = mvsEncoders(index)
      try {
        dest.put(encoder.encodeValue(fieldValue))
      } catch {
        case t: Throwable =>
          return Result.Failure(s"Failed to encode field:" + t.getMessage +
            s" fieldId=$index encoder=$encoder value=${fieldValue.getValue}")
      }
      index += 1
    }
    Result.Success
  }

  def encodeValueAsAWholeRow(typ: StandardSQLTypeName,
                              value: FieldValue,
                              transcoder: Transcoder,
                              buf: ByteBuffer): Unit = {
    CloudLogging.stdout(s"MVSBinaryRowEncoder Encoding ${value.getValue} of BQ type: $typ.")
    typ match {
      case StandardSQLTypeName.STRING =>
        if (!value.isNull) {
          val str = value.getStringValue
          val payLoad = StringToBinaryEncoder(transcoder, str.size).encode(str)
          buf.put(payLoad, buf.position(), str.size)
        } else {
          buf.put(StringToBinaryEncoder(transcoder, 1).encode(null))
        }
      case StandardSQLTypeName.FLOAT64 =>
        if (!value.isNull) {
          val typedVal = new java.math.BigDecimal(String.valueOf(value.getValue.asInstanceOf[Double]))
          val long =  java.lang.Long.parseLong(typedVal.toString.replace(".", ""))
          buf.put(DecimalToBinaryEncoder(typedVal.precision(), typedVal.scale()).encode(long))
        } else {
          val encoder = DecimalToBinaryEncoder(5, 0).encode(null)
          buf.put(encoder, buf.position(), encoder.size)
        }
      case StandardSQLTypeName.INT64 =>
        val encoder = LongToBinaryEncoder(4)
        if (!value.isNull) buf.put(encoder.encode(value.getLongValue), buf.position(), encoder.size)
        else buf.put(encoder.encode(null), buf.position(), encoder.size)
      case StandardSQLTypeName.BYTES =>
        if (value.isNull) buf.put(BytesToBinaryEncoder(1).encode(null), buf.position(), 1)
        else {
          val bytes = value.getBytesValue
          buf.put(BytesToBinaryEncoder(bytes.size).encode(bytes), buf.position(), bytes.size)
        }
      case StandardSQLTypeName.DATE =>
        if (value.isNull) buf.put(DateStringToBinaryEncoder().encode(null), buf.position(), 4)
        else buf.put(DateStringToBinaryEncoder().encode(value.getStringValue), buf.position(), 4)
    }
  }
}
