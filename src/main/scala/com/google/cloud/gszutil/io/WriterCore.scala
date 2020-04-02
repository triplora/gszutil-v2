package com.google.cloud.gszutil.io

import java.nio.ByteBuffer

import com.google.cloud.gszutil.PackedDecimal

class WriterCore(args: V2WriterArgs, name: String) {
  val orc = new OrcContext(args.gcs, args.schemaProvider.ORCSchema,
    args.basePath, name, args.partitionBytes, args.pool)
  val BatchSize = 1024
  private val reader = new ZReader(args.schemaProvider, BatchSize)
  private val errBuf = ByteBuffer.allocate(args.schemaProvider.LRECL * BatchSize)
  private var errorCount: Long = 0
  private var bytesIn: Long = 0
  def getBytesIn: Long = bytesIn
  def getErrorCount: Long = errorCount

  def write(buf: ByteBuffer): WriteResult = {
    bytesIn += buf.limit
    val res = orc.write(reader, buf, errBuf)
    errorCount += res.errCount
    if (errBuf.position > 0){
      errBuf.flip()
      val a = new Array[Byte](args.schemaProvider.LRECL)
      errBuf.get(a)
      System.err.println(s"Failed to read row")
      System.err.println(PackedDecimal.hexValue(a))
    }
    res
  }

  def close(): Unit = {
    orc.flush()
    orc.close()
  }
}
