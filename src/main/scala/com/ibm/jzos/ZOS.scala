/*
 * Copyright 2019 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.jzos

import java.nio.ByteBuffer
import java.security.Security

import com.google.cloud.gszutil.Util.{Logging, ZInfo}
import com.google.cloud.gszutil.io.ZRecordReaderT

import scala.util.Try

/**  Calls and wraps IBM JZOS classes
  *
  */
protected object ZOS extends Logging {
  class RecordReaderCloser(r: RecordReader) extends Thread {
    override def run(): Unit = {
      try {
        logger.info("Closing " + r.getDDName + " " + r.getDsn)
        r.close()
      } catch {
        case e: ZFileException =>
          logger.error(s"failed to close ${r.getDDName}", e)
      }
    }
  }

  class WrappedRecordReader(private val r: RecordReader) extends ZRecordReaderT with Logging {
    require(r.getRecfm == "FB", s"${r.getDDName} record format must be FB - ${r.getRecfm} is not supported")

    private var open = true
    private var nRecordsRead: Long = 0

    override def getDsn: String = r.getDsn

    @scala.inline
    override def read(buf: Array[Byte]): Int =
      read(buf, 0, buf.length)

    @scala.inline
    override def read(buf: Array[Byte], off: Int, len: Int): Int = {
      nRecordsRead += 1
      r.read(buf, off, len)
    }

    override def close(): Unit = {
      if (open) {
        open = false
        logger.info("Closing " + r.getDDName + " " + r.getDsn)
        Try(r.close()).failed.foreach(t => logger.error(t.getMessage))
      }
    }

    override def isOpen: Boolean = open
    override val lRecl: Int = r.getLrecl
    override val blkSize: Int = r.getBlksize
    private val buf: Array[Byte] = new Array[Byte](lRecl)

    @scala.inline
    override def read(dst: ByteBuffer): Int = {
      val n = read(buf, 0, lRecl)
      val k = math.max(0,n)
      dst.put(buf, 0, k)
      n
    }

    /** Number of records read
      *
      */
    override def count(): Long = nRecordsRead
  }

  class WrappedVBRecordReader(private val r: RecordReader) extends ZRecordReaderT with Logging {
    require(r.getRecfm == "VB", s"${r.getDDName} record format must be FB - ${r.getRecfm} is not " +
      s"supported")

    override val lRecl: Int = r.getLrecl
    override val blkSize: Int = r.getBlksize

    private val buf: Array[Byte] = new Array[Byte](lRecl)
    private var open = true
    private var nRecordsRead: Long = 0

    override def isOpen: Boolean = open
    override def count(): Long = nRecordsRead
    override def getDsn: String = r.getDsn

    @scala.inline
    override def read(buf: Array[Byte]): Int = {
      nRecordsRead += 1
      r.read(buf)
    }

    @scala.inline
    override def read(buf: Array[Byte], off: Int, len: Int): Int =
      read(buf)

    override def close(): Unit = {
      if (open) {
        open = false
        logger.info("Closing " + r.getDDName + " " + r.getDsn)
        Try(r.close()).failed.foreach(t => logger.error(t.getMessage))
      }
    }

    @scala.inline
    override def read(dst: ByteBuffer): Int = {
      val n = read(buf)
      val k = math.max(0,n)
      dst.put(buf, 0, k)
      n
    }
  }

  def ddExists(ddName: String): Boolean = {
    ZFile.ddExists(ddName)
  }

  def readDD(ddName: String): ZRecordReaderT = {
    logger.debug(s"reading DD $ddName")
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    try {
      val reader: RecordReader = RecordReader.newReaderForDD(ddName)
      logger.info(s"Reading DD $ddName with ${reader.getClass.getSimpleName}\nDSN=${reader.getDsn}\nRECFM=${reader.getRecfm}\nBLKSIZE=${reader.getBlksize}\nLRECL=${reader.getLrecl}")

      if (reader.getRecfm == "FB")
        new WrappedRecordReader(reader)
      else if (reader.getRecfm == "VB")
        new WrappedVBRecordReader(reader)
      else
        throw new RuntimeException(s"Unsupported record format: '${reader.getRecfm}'")
    } catch {
      case e: ZFileException =>
        throw new RuntimeException(s"Failed to open DD:'$ddName'", e)
    }
  }

  def addCCAProvider(): Unit = {
    Security.insertProviderAt(new com.ibm.crypto.hdwrCCA.provider.IBMJCECCA(), 1)
  }

  def getJobName: String = ZUtil.getCurrentJobname

  def getInfo: ZInfo = ZInfo(
    ZUtil.getCurrentJobId,
    ZUtil.getCurrentJobname,
    ZUtil.getCurrentStepname,
    ZUtil.getCurrentUser
  )

  def substituteSystemSymbols(s: String): String = ZUtil.substituteSystemSymbols(s)
}
