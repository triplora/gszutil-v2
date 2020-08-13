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
package com.google.cloud.imf.gzos

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.Security
import java.util.Date

import com.google.cloud.gszutil.Decoding
import com.google.cloud.gszutil.io.{ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.imf.gzos.MVSStorage.DSN
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.ibm.jzos.{DSCB, Exec, Format1DSCB, Format3DSCB, JesSymbols, MvsJobSubmitter, PdsDirectory, RcException, RecordReader, RecordWriter, ZFile, ZFileConstants, ZFileException, ZUtil}

import scala.util.Try

/**  Calls and wraps IBM JZOS classes
  *
  */
protected object ZOS {
  class WrappedRecordReader(private val r: RecordReader) extends ZRecordReaderT with Logging {
    require(r.getRecfm.startsWith("F"),
      s"${r.getDDName} record format must be FB - ${r.getRecfm} " +
      s"is not supported")

    private var open = true
    private var nRecordsRead: Long = 0

    override def getDsn: String = r.getDsn

    @scala.inline
    override final def read(buf: Array[Byte]): Int =
      read(buf, 0, buf.length)

    @scala.inline
    override final def read(buf: Array[Byte], off: Int, len: Int): Int = {
      nRecordsRead += 1
      r.read(buf, off, len)
    }

    override def close(): Unit = {
      if (open) {
        open = false
        System.out.println("Closing " + r.getDDName + " " + r.getDsn)
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
    require(r.getRecfm == "VB",
      s"${r.getDDName} ${r.getDsn} record format=${r.getRecfm} but expected VB")
    require(r.getClass.getSimpleName.stripSuffix("$") == "BsamVRecordReader")

    // record length without
    override val lRecl: Int = r.getLrecl - 4
    override val blkSize: Int = r.getBlksize

    private var open = true
    private var nRecordsRead: Long = 0

    override def isOpen: Boolean = open
    override def count(): Long = nRecordsRead
    override def getDsn: String = r.getDsn

    @scala.inline
    override final def read(buf: Array[Byte]): Int = read(buf, 0, buf.length)

    @scala.inline
    override final def read(buf: Array[Byte], off: Int, len: Int): Int = {
      val n = r.read(buf, off, len)
      if (n > 0) {
        nRecordsRead += 1
        var i = off + n
        val limit = off + lRecl
        while (i < limit) {
          buf(i) = Decoding.EBCDICSP
          i += 1
        }
        lRecl
      } else n
    }

    override def close(): Unit = {
      if (open) {
        open = false
        System.out.println("Closing " + r.getDDName + " " + r.getDsn)
        Try(r.close()).failed.foreach(t => logger.error(t.getMessage))
      }
    }

    @scala.inline
    override def read(dst: ByteBuffer): Int = {
      val n = read(dst.array(), dst.position(), dst.remaining())
      if (n > 0) {
        val startPos = dst.position()
        dst.position(startPos + n)
      }
      n
    }
  }

  class WrappedRecordWriter(private val w: RecordWriter) extends ZRecordWriterT {
    override val lRecl: Int = w.getLrecl
    override val blkSize: Int = w.getBlksize

    private var open = true
    private var nRecordsWritten: Long = 0
    private val buf: Array[Byte] = new Array[Byte](lRecl)

    override def write(src: Array[Byte]): Unit = {
      w.write(src)
      nRecordsWritten += 1
    }

    override def write(src: ByteBuffer): Int = {
      src.get(buf)
      w.write(buf)
      nRecordsWritten += 1
      buf.length
    }

    override def isOpen: Boolean = open

    override def flush(): Unit = w.flush()

    override def close(): Unit = {
      if (open) {
        open = false
        System.out.println("Closing " + w.getDDName + " " + w.getDsn)
        w.flush()
        Try(w.close()).failed.foreach{t =>
          System.err.println(t.getMessage)
          t.printStackTrace(System.err)
        }
      }
    }

    /** Number of records read */
    override def count(): Long = nRecordsWritten

    /** DSN */
    override def getDsn: String = w.getDsn
  }

  class RecordIterator(val r: ZRecordReaderT,
                       limit: Long = 100000,
                       charset: Charset = Ebcdic.charset)
    extends Iterator[String] with AutoCloseable {
    private val buf = new Array[Byte](r.lRecl)
    private var n = 0
    private var count: Long = 0
    private var closed = false

    override def hasNext: Boolean = n > -1

    override def next(): String = {
      n = r.read(buf)
      if (n > -1 && count < limit) {
        if (n == r.lRecl){
          count += 1
          new String(buf,charset)
        } else {
          throw new IOException(s"RecordReader read $n bytes but expected ${r.lRecl}")
        }
      } else {
        if (count >= limit)
          System.err.println(s"${r.getDsn} exceeded $limit record limit")
        close()
        null
      }
    }

    override def close(): Unit =
      if (!closed) {
        r.close()
        closed = true
      }
  }

  def exists(dsn: DSN): Boolean = ZFile.exists(dsn.fqdsn)
  def ddExists(ddName: String): Boolean = ZFile.ddExists(ddName)

  def readDSN(dsn: DSN): ZRecordReaderT = {
    System.out.println(s"reading DSN $dsn")
    //if (!ZFile.dsExists(dsn))
    //  throw new RuntimeException(s"DSN $dsn does not exist")

    try {
      val reader = RecordReader.newReader(dsn.fqdsn, ZFileConstants.FLAG_DISP_SHR)
      System.out.println(
        s"""Reading DSN $dsn with ${reader.getClass.getSimpleName}
           |DSN=${reader.getDsn}
           |RECFM=${reader.getRecfm}
           |BLKSIZE=${reader.getBlksize}
           |LRECL=${reader.getLrecl}""".stripMargin)

      if (reader.getRecfm.startsWith("F"))
        new WrappedRecordReader(reader)
      else if (reader.getRecfm == "VB")
        new WrappedVBRecordReader(reader)
      else
        throw new RuntimeException(s"Unsupported record format: '${reader.getRecfm}'")
    } catch {
      case e: ZFileException =>
        throw new RuntimeException(s"Failed to open DD:'$dsn'", e)
    }
  }

  def writeDSN(dsn: DSN): ZRecordWriterT =
    new WrappedRecordWriter(RecordWriter.newWriter(dsn.fqdsn, ZFileConstants.FLAG_DISP_SHR))

  def writeDD(ddName: String): ZRecordWriterT =
    new WrappedRecordWriter(RecordWriter.newWriterForDD(ddName))

  def readDD(ddName: String): ZRecordReaderT = {
    System.out.println(s"reading DD $ddName")
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    try {
      val reader: RecordReader = RecordReader.newReaderForDD(ddName)
      System.out.println(s"Reading DD $ddName with ${reader.getClass.getSimpleName}\nDSN=${reader.getDsn}\nRECFM=${reader.getRecfm}\nBLKSIZE=${reader.getBlksize}\nLRECL=${reader.getLrecl}")

      if (reader.getRecfm.startsWith("F"))
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

  def addCCAProvider(): Unit =
    Security.insertProviderAt(new com.ibm.crypto.hdwrCCA.provider.IBMJCECCA(), 1)

  def getJobId: String = ZUtil.getCurrentJobId
  def getJobName: String = ZUtil.getCurrentJobname

  def getSymbol(s: String): Option[String] =
    Option(JesSymbols.extract(s).get(s))

  def getSymbols: Map[String,String] = {
    import scala.jdk.CollectionConverters.MapHasAsScala
    JesSymbols.extract("*").asScala.toMap
  }

  def getDSN(dd: String): String = {
    val rr = RecordReader.newReaderForDD(dd)
    val dsn = rr.getDsn
    rr.close()
    dsn
  }

  def getDSNInfo(dsn: String): DSCBChain = {
    val volume = ZFile.locateDSN(dsn).headOption.getOrElse("")
    new DSCBChain(ZFile.readDSCBChain(dsn, volume))
  }

  class DSCBChain(val chain: Array[DSCB]) {
    val f1Extents: Int = chain.find(_.isInstanceOf[Format1DSCB])
      .map(_.asInstanceOf[Format1DSCB])
      .map(_.getDS1NOEPV)
      .getOrElse(0)
    val f3Count: Int = chain.count(_.isInstanceOf[Format3DSCB])
  }

  class PDSMember(val info: PdsDirectory.MemberInfo) extends PDSMemberInfo {
    override def name: String = info.getName
    override def currentLines: Int = info.getStatistics.currentLines
    override def toString: String =
      s"""name:     $name
         |created:  $creationDate
         |modified: $modificationDate
         |userid:   $userId
         |version:  $version
         |lines:    $currentLines
         |""".stripMargin

    override def creationDate: Date = info.getStatistics.creationDate
    override def modificationDate: Date = info.getStatistics.modificationDate
    override def userId: String = info.getStatistics.userid
    override def version: Int = info.getStatistics.version
  }

  /** Partitioned Data Set Iterator
    * @param dsn DSN in format //'HLQ.MEMBER'
    */
  class PDSIterator(val dsn: DSN) extends Iterator[PDSMemberInfo] {
    private val dir = new PdsDirectory(dsn.fqdsn)
    import scala.jdk.CollectionConverters.IteratorHasAsScala
    private val iter = dir.iterator().asScala

    override def hasNext: Boolean = iter.hasNext
    override def next(): PDSMember = new PDSMember(iter.next()
      .asInstanceOf[PdsDirectory.MemberInfo])
  }

  def getInfo: ZOSJobInfo = ZOSJobInfo.newBuilder
    .setJobid(ZUtil.getCurrentJobId)
    .setJobdate(sys.env.getOrElse("JOBDATE","UNKNOWN"))
    .setJobtime(sys.env.getOrElse("JOBTIME","UNKNOWN"))
    .setJobname(ZUtil.getCurrentJobname)
    .setStepName(ZUtil.getCurrentStepname)
    .setProcStepName(ZUtil.getCurrentProcStepname)
    .setUser(ZUtil.getCurrentUser)
    .build

  def substituteSystemSymbols(s: String): String = ZUtil.substituteSystemSymbols(s)

  def submitJCL(jcl: Seq[String]): Option[MVSJob] = {
    // job name is first non-comment line
    val jobName = jcl.find(x => x.startsWith("//") && !x.startsWith("//*"))
      .map(s => s.substring(2, s.indexOf(' ')))
    if (jobName.isDefined) {
      val mjs = new MvsJobSubmitter()
      val lrecl = mjs.getRdrLrecl
      val padByte = " ".getBytes(ZUtil.getDefaultPlatformEncoding)
      val buf = ByteBuffer.allocate(lrecl)

      for (line <- jcl) {
        buf.clear
        buf.put(line.getBytes(ZUtil.getDefaultPlatformEncoding))
        while (buf.hasRemaining)
          buf.put(padByte)
        mjs.write(buf.array)
      }

      mjs.close()
      Option(new MVSJob(jobName.get, mjs.getJobid))
    } else None
  }

  def envWithBPX: Array[String] = {
    import scala.jdk.CollectionConverters.PropertiesHasAsScala
    val p = ZUtil.getEnvironment.asScala
    p.put("_BPX_SHAREAS", "YES")
    p.put("_BPX_SPAWN_SCRIPT", "YES")
    p.map{x => s"${x._1} = ${x._2}"}.toArray
  }

  def execCmd(cmd: String): (Int,Iterator[String]) = {
    val exec = new Exec(cmd, envWithBPX)
    exec.run()
    exec.getStdinWriter.close()
    (exec.getReturnCode, lines(exec))
  }

  def lines(exec: Exec): Iterator[String] = {
    new LineIterator(exec).takeWhile(_ != null)
  }

  class MVSJob(jobName: String, jobId: String) extends ZMVSJob {
    def getStatus: String = {
      val cmd = s"jobStatus $jobId"
      val (rc,lines) = ZOS.execCmd(cmd)

      if (rc != 0) {
        throw new RcException("jobStatus failed", rc)
      } else {
        val stdout = lines.toArray.toSeq
        if (stdout.isEmpty)
          throw new IOException("No output from jobStatus child process")
        stdout.last
      }
    }

    def getOutput: Seq[String] = {
      val cmd = s"jobOutput $jobName $jobId"
      val (rc,lines) = ZOS.execCmd(cmd)
      if (rc != 0) throw new RcException("jobOutput failed", rc)
      else lines.toArray.toSeq
    }
  }

  class LineIterator(val e: Exec) extends Iterator[String] {
    private var open = true
    override def hasNext: Boolean = open
    override def next(): String = {
      val line = e.readLine()
      if (line == null) open = false
      line
    }
  }
}
