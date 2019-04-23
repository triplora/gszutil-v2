package com.google.cloud.gszutil


import java.nio.charset.Charset

import com.google.cloud.gszutil.ZReader.TRecordReader
import com.ibm.jzos.{RecordReader, ZFile, ZUtil}

object ZOS {
  def getDefaultCharset: Charset = Charset.forName(ZUtil.getDefaultPlatformEncoding)

  class WrappedRecordReader(r: RecordReader) extends TRecordReader {
    override def read(buf: Array[Byte]): Int =
      r.read(buf)
    override def read(buf: Array[Byte], off: Int, len: Int): Int =
      r.read(buf, off, len)
    override def close(): Unit = r.close()
    override def getLrecl: Int = r.getLrecl
  }

  def readDD(ddName: String): TRecordReader = {
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    val reader = RecordReader.newReaderForDD(ddName)
    reader.setAutoFree(true)
    System.out.println(s"Reading DD $ddName ${reader.getDsn} with record format ${reader.getRecfm} BLKSIZE ${reader.getBlksize} LRECL ${reader.getLrecl} with default system encoding ${ZUtil.getDefaultPlatformEncoding}")
    new WrappedRecordReader(reader)
  }
}
