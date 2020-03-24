package com.google.cloud.gszutil

import com.google.cloud.gzos.Ebcdic
import com.google.cloud.gzos.pb.Schema.{Field, Record}

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
  override def LRECL: Int = {
    val lrecl = decoders.foldLeft(0){_ + _.size}
    if (r.getVartext) lrecl + decoders.length - 1
    else lrecl
  }

  override def vartext: Boolean = r.getVartext
}

