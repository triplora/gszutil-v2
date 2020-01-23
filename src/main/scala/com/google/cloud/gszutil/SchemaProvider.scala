package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.Decoder
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

trait SchemaProvider {
  def fieldNames: Seq[String]

  def decoders: Array[Decoder]

  def toByteArray: Array[Byte]

  def typeDescriptions: Array[TypeDescription] =
    decoders.flatMap(_.typeDescription)

  def ORCSchema: TypeDescription = {
    val schema = new TypeDescription(Category.STRUCT)
    fieldNames
      .zip(typeDescriptions)
      .foreach{ f => schema.addField(f._1, f._2) }
    schema
  }

  def LRECL: Int = decoders.foldLeft(0){_ + _.size}
}
