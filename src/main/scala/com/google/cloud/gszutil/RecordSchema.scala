package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.Decoder
import com.google.cloud.gzos.pb.Schema.{Field, Record}

case class RecordSchema(r: Record) extends SchemaProvider {
  import scala.collection.JavaConverters._
  private def fields: Array[Field] = r.getFieldList.asScala.toArray
  override def fieldNames: Seq[String] = fields.filterNot(_.getFiller).map(_.getName)
  override lazy val decoders: Array[Decoder] = fields.map(Decoding.getDecoder)
  override def toByteArray: Array[Byte] = r.toByteArray
}

