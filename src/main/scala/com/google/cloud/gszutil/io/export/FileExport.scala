package com.google.cloud.gszutil.io.`export`

import com.google.cloud.gszutil.Transcoder

trait FileExport {
  def close(): Unit
  def lRecl: Int
  def appendBytes(buf: Array[Byte]): Unit
  def ddName: String
  def transcoder: Transcoder
  def rowsWritten(): Long
}
