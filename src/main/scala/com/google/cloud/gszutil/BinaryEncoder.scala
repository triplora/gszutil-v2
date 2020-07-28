package com.google.cloud.gszutil

import com.google.cloud.bigquery.StandardSQLTypeName

trait BinaryEncoder extends {
  def size: Int
  def bqSupportedType: StandardSQLTypeName
}