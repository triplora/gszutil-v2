package com.google.cloud.gszutil

import java.nio.charset.Charset

import com.google.cloud.gzos.Ebcdic
import com.google.cloud.gzos.pb.Schema.{Field, Record}
import com.google.common.base.Charsets

case class RecordSchema(r: Record) extends SchemaProvider {
  import scala.collection.JavaConverters._
  private def fields: Array[Field] = r.getFieldList.asScala.toArray
  override def fieldNames: Seq[String] = fields.filterNot(_.getFiller).map(_.getName)
  override lazy val decoders: Array[Decoder] = {
    if (r.getVartext) {
      val delimiter = r.getDelimiter.toByteArray
      fields.map(VartextDecoding.getVartextDecoder(_, delimiter, transcoder))
    } else fields.map(Decoding.getDecoder(_, transcoder))
  }
  private def transcoder: Transcoder = if (r.getEncoding == "") Ebcdic else Utf8
  override def toByteArray: Array[Byte] = r.toByteArray
}

