package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.{CopyBookField, CopyBookLine, Decoder, parseCopyBookLine}
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.mutable.ArrayBuffer


case class CopyBook(raw: String) {
  final val Fields: Seq[CopyBookLine] = raw.lines.flatMap(parseCopyBookLine).toSeq

  final val FieldNames: Seq[String] =
    Fields.flatMap{
      case CopyBookField(name, _) =>
        Option(name.replaceAllLiterally("-","_"))
      case _ =>
        None
    }

  def getDecoders: Array[Decoder[_]] = {
    val buf = ArrayBuffer.empty[Decoder[_]]
    Fields.foreach{
      case CopyBookField(_, pic) =>
        buf.append(pic.getDecoder)
      case _ =>
    }
    buf.toArray
  }

  final val ORCSchema: TypeDescription = {
    val schema = new TypeDescription(Category.STRUCT)
    FieldNames
      .zip(getDecoders)
      .foreach{f =>
        schema.addField(f._1, f._2.typeDescription)
      }
    schema
  }

  final val LRECL: Int = getDecoders.foldLeft(0){_ + _.size}
}

