package com.google.cloud.gszutil

import com.google.cloud.bigquery.StandardSQLTypeName

trait BinaryEncoder extends {
  type T
  def encode(elem: T): Array[Byte]
  def size: Int
  def bqSupportedType: StandardSQLTypeName
}