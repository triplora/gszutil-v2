package com.google.cloud.gszutil

import java.nio.charset.Charset

import com.google.cloud.gzos.Ebcdic
import com.google.cloud.gzos.pb.Schema.{Field, Record}

case class RecordSchema(r: Record) extends SchemaProvider {
  import scala.collection.JavaConverters._
  private def fields: Array[Field] = r.getFieldList.asScala.toArray
  override def fieldNames: Seq[String] = fields.filterNot(_.getFiller).map(_.getName)
  override lazy val decoders: Array[Decoder] =
    if (r.getVartext)
      fields.map(VartextDecoding.getVartextDecoder(_, transcoder))
    else
      fields.map(Decoding.getDecoder(_, transcoder))

  override def vartextDecoders: Array[VartextDecoder] = {
    if (vartext) decoders.flatMap{
      case x: VartextDecoder => Some(x)
      case _ => None
    }
    else throw new RuntimeException("record is not stored as vartext")
  }

  private def transcoder: Transcoder =
    if (r.getEncoding == "" || r.getEncoding.equalsIgnoreCase("EBCDIC"))
      Ebcdic
    else Utf8

  override def toByteArray: Array[Byte] = r.toByteArray
  override def LRECL: Int = {
    if (r.getVartext) r.getLrecl
    else decoders.foldLeft(0){_ + _.size}
  }

  override def vartext: Boolean = r.getVartext
  override def delimiter: Array[Byte] = r.getDelimiter.toByteArray
  override def srcCharset: Charset = transcoder.charset
}

