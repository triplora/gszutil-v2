package com.google.cloud.gszutil.io.`export`

trait FileExport {
  def close(): Unit
  def lRecl: Int
  def recfm: String
  def appendBytes(buf: Array[Byte]): Unit
  def rowsWritten(): Long
}
