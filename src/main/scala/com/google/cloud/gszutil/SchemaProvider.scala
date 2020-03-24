package com.google.cloud.gszutil

import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

trait SchemaProvider {
  def fieldNames: Seq[String]

  def decoders: Array[Decoder]

  def toByteArray: Array[Byte]

  def ORCSchema: TypeDescription =
    fieldNames.zip(decoders.filterNot(_.filler))
      .foldLeft(new TypeDescription(Category.STRUCT)){(a,b) =>
          a.addField(b._1,b._2.typeDescription)
      }

  def vartext: Boolean = false

  def LRECL: Int = decoders.foldLeft(0){_ + _.size}

  override def toString: String =
    decoders.map{x => s"${x.toString} (${x.filler})"}.mkString("\n")
}
