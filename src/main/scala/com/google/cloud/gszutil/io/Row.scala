package com.google.cloud.gszutil.io

class Row(val fieldNames: Seq[String]) {
  val nameMap: Map[String,Int] = fieldNames.zipWithIndex.toMap
  val values: Array[Any] = new Array[Any](fieldNames.length)
  val isNull: Array[Boolean] = new Array[Boolean](fieldNames.length)
}
