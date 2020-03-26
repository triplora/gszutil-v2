package com.google.cloud.gszutil

import java.nio.charset.Charset

import com.google.cloud.gzos.Ebcdic
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

trait SchemaProvider {
  def fieldNames: Seq[String]

  def decoders: Array[Decoder]
  def vartextDecoders: Array[VartextDecoder] = Array.empty

  def toByteArray: Array[Byte]

  def ORCSchema: TypeDescription =
    fieldNames.zip(decoders.filterNot(_.filler))
      .foldLeft(new TypeDescription(Category.STRUCT)){(a,b) =>
          a.addField(b._1,b._2.typeDescription)
      }

  def vartext: Boolean = false

  def delimiter: Array[Byte]

  def srcCharset: Charset = Ebcdic.charset

  def LRECL: Int = decoders.foldLeft(0){_ + _.size}

  override def toString: String =
    decoders.map{x => s"${x.toString} (${x.filler})"}.mkString("\n")
}
