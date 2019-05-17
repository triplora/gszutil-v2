package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.{CopyBookField, Decoder, parseCopyBookLine}
import com.google.cloud.gszutil.io.ZReader
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.mutable.ArrayBuffer


case class CopyBook(raw: String) {
  @transient
  lazy val lines = raw.lines.flatMap(parseCopyBookLine).toSeq

  @transient
  lazy val getFieldNames: Seq[String] =
    lines.flatMap{
      case CopyBookField(name, _) =>
        Option(name.replaceAllLiterally("-","_"))
      case _ =>
        None
    }

  @transient
  lazy val getDecoders: Seq[Decoder[_]] = {
    val buf = ArrayBuffer.empty[Decoder[_]]
    lines.foreach{
      case CopyBookField(_, pic) =>
        val decoder = pic.getDecoder
        buf.append(decoder)
      case _ =>
    }
    buf.result.toArray.toSeq
  }

  @transient
  lazy val getOrcSchema: TypeDescription = {
    val schema = new TypeDescription(Category.STRUCT)
    getFieldNames
      .zip(getDecoders)
      .foreach{f =>
        schema.addField(f._1, f._2.typeDescription)
      }
    schema
  }

  @transient
  lazy val lRecl: Int = getDecoders.foldLeft(0){_ + _.size}

  def reader: ZReader = new ZReader(this)
}

