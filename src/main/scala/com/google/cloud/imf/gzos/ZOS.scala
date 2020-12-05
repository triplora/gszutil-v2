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

import java.io.{IOException, PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.charset.Charset
import java.security.Security
import java.util.Date

import com.google.cloud.gszutil.Decoding
import com.google.cloud.gszutil.io.{ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.imf.gzos.MVSStorage.DSN
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.imf.util.{CloudLogging, Logging}
import com.ibm.dataaccess.ByteArrayUnmarshaller
import com.ibm.jzos.{ByteUtil, DSCB, Exec, Format1DSCB, Format3DSCB, JesSymbols, MvsJobSubmitter, PdsDirectory, RcException, RecordReader, RecordWriter, ZFile, ZFileConstants, ZFileException, ZUtil}

import scala.collection.mutable.ListBuffer
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
        CloudLogging.stdout("Closing " + r.getDDName + " " + r.getDsn)
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
        CloudLogging.stdout("Closing " + r.getDDName + " " + r.getDsn)
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
        CloudLogging.stdout("WrappedRecordWriter Closing " + w.getDDName + " " + w.getDsn)
        w.flush()
        Try(w.close()).failed.foreach{t =>
          val msg = "WrappedRecordWriter ERROR " + t.getMessage
          CloudLogging.stdout(msg)
          CloudLogging.stderr(msg)
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
        if (count >= limit) {
          val msg = s"RecordIterator ERROR ${r.getDsn} exceeded $limit record limit"
          CloudLogging.stdout(msg)
          CloudLogging.stderr(msg)
        }
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
    CloudLogging.stdout(s"Opening RecordReader for $dsn")
    try {
      val reader = RecordReader.newReader(dsn.fqdsn, ZFileConstants.FLAG_DISP_SHR)
      CloudLogging.stdout(
        s"""Opened ${reader.getClass.getSimpleName}
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
      case e: RcException if e.getMessage != null && e.getMessage.contains("BPXWDYN failed") =>
        val sb = new StringBuilder
        sb.append(s"DSN=$dsn doesn't exist\n")
        sb.append(e.getMessage)
        val msg = sb.result
        CloudLogging.stdout(msg)
        CloudLogging.printStackTrace(e)
        throw new RuntimeException(msg, e)
      case e: Throwable =>
        val sb = new StringBuilder
        sb.append(s"Failed to open DSN=$dsn\n")
        if (e.getMessage != null) sb.append(e.getMessage)
        val msg = sb.result
        CloudLogging.stdout(msg)
        throw new RuntimeException(msg, e)
    }
  }

  def writeDSN(dsn: DSN, lrecl: Int, space1: Int, space2: Int, in: ReadableByteChannel): Unit = {
    val ddname = ZFile.allocDummyDDName()
    val cmd =
      s"alloc fi($ddname) da(${dsn.fqdsn}) reuse new catalog msg(2)" +
      s" recfm(f,b) space($space1,$space2) cyl lrecl($lrecl)"
    try {
      ZFile.bpxwdyn(cmd)
    } catch {
      case e: RcException =>
        val msg = s"Failed to allocate DD for $dsn\n$cmd"
        throw new RuntimeException(msg, e)
    }

    var writer: RecordWriter = null
    try {
      writer = RecordWriter.newWriterForDD(ddname)
      assert(writer.getLrecl == lrecl, s"writer lrecl = ${writer.getLrecl} but expected $lrecl")
      val buf = ByteBuffer.allocate(lrecl*1024)
      var n = 0
      while (n > -1) {
        // fill buffer
        while (buf.remaining > lrecl && n > -1){
          n = in.read(buf)
        }

        // prepare buffer for reads
        buf.flip()

        while (buf.remaining() >= lrecl) {
          val pos0 = buf.position()
          val pos1 = pos0 + lrecl
          // write record
          writer.write(buf.array(), pos0, lrecl)
          buf.position(pos1)
        }

        // prepare buffer for writes
        buf.compact()
      }
    } finally {
      if (writer != null) {
        try {
          writer.close()
        } catch {
          case e: ZFileException =>
            val sw = new StringWriter()
            val pw = new PrintWriter(sw)
            e.printStackTrace(pw)
            val msg = s"Failed to close writer for $ddname\n" + e.getMessage +"\n" + sw.toString
            CloudLogging.stdout(msg)
            CloudLogging.stderr(msg)
        }
      }
      try {
        ZFile.bpxwdyn(s"free fi($ddname) msg(2)")
      } catch {
        case e: RcException =>
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          e.printStackTrace(pw)
          val msg = s"Failed to free DD $ddname\n" + e.getMessage +"\n" + sw.toString
          CloudLogging.stdout(msg)
          CloudLogging.stderr(msg)
      }
    }
  }

  def writeDSN(dsn: DSN): ZRecordWriterT =
    new WrappedRecordWriter(RecordWriter.newWriter(dsn.fqdsn, ZFileConstants.FLAG_DISP_SHR))

  def writeDD(ddName: String): ZRecordWriterT =
    new WrappedRecordWriter(RecordWriter.newWriterForDD(ddName))

  class DDException(msg: String) extends IOException(msg)

  /** Opens a DD for reading and returns a RecordReader
    * uses native z/OS datasets only
    * throws an exception if the DD is not defined
    * @param ddName name of DD
    * @return ZRecordReaderT
    */
  def readDD(ddName: String): ZRecordReaderT = {
    CloudLogging.stdout(s"Reading DD:$ddName")

    try {
      val reader: RecordReader = RecordReader.newReaderForDD(ddName)
      CloudLogging.stdout(
        s"""Opened ${reader.getClass.getSimpleName}
           |DSN=${reader.getDsn}
           |RECFM=${reader.getRecfm}
           |BLKSIZE=${reader.getBlksize}
           |LRECL=${reader.getLrecl}""".stripMargin)

      if (reader.getDsn == "NULLFILE") {
        // Close the dataset to avoid SC03 Abend
        reader.close()
        val msg = s"DD:$ddName not found"
        CloudLogging.stdout(msg)
        CloudLogging.stderr(msg)
        throw new DDException(msg)
      }

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

  /** Obtain Data Set information without opening file
    * @param ddName DDNAME to obtain information for
    * @return DataSetInfo
    */
  def getDatasetInfo(ddName: String): Option[DataSetInfo] = {
    try {
      val jfcb = ZFile.readJFCB(ddName)
      val dsn =
        if (jfcb.getJfcbelnm.isEmpty) jfcb.getJfcbdsnm
        else s"${jfcb.getJfcbdsnm}(${jfcb.getJfcbelnm})"
      val lrecl =
        if (jfcb.getJfclrecl == 0) 80
        else {
          jfcb.getJfclrecl
        }
      Option(DataSetInfo(dsn = dsn, lrecl = lrecl))
    } catch {
      case _: ZFileException =>
        None
    }
  }

  def readAddr(buf: Array[Byte], off: Int): Long =
    ByteArrayUnmarshaller.readInt(buf, off, true, 4, true)

  def readInt(address: Long, len: Int): Int = {
    if (len > 4) throw new IllegalArgumentException("len > 8")
    else if (len <= 0) throw new IllegalArgumentException("len <= 0")
    else {
      val bytes = new Array[Byte](len)
      ZUtil.peekOSMemory(address, bytes)
      ByteUtil.bytesAsInt(bytes)
    }
  }

  def readStr(address: Long, len: Int): String = {
    if (len <= 0) throw new IllegalArgumentException("len <= 0")
    else {
      val bytes = new Array[Byte](len)
      ZUtil.peekOSMemory(address, bytes)
      new String(bytes, 0, len, Ebcdic.charset).trim
    }
  }

  def listDDs: Seq[String] = {
    val pCVT = ZUtil.peekOSMemory(16L, 4)
    val pTCBW = ZUtil.peekOSMemory(pCVT + 0L, 4)
    val pTCB = ZUtil.peekOSMemory(pTCBW + 4L, 4)
    val pTIOT = ZUtil.peekOSMemory(pTCB + 12L, 4)
    val pJSCB = ZUtil.peekOSMemory(pTCB + 180L, 4)
    val pSSIB = ZUtil.peekOSMemory(pJSCB + 316L, 4)
    val pPSCB = ZUtil.peekOSMemory(pJSCB + 264L, 4)

    val jobName = readStr(pTIOT, 8)
    val procStepName = readStr(pTIOT+8, 8)
    val stepName = readStr(pTIOT+16, 8)
    val jobId = readStr(pSSIB+12,8)

    // read TIOENTRY
    var i = pTIOT + 24
    val buf = ListBuffer.empty[String]
    var n = 0
    while (i < pTIOT + 32*1024 && n < 256) {
      n += 1
      val len = readInt(i, 1)
      if (len > 0){
        val ddName = readStr(i+4,8)
        if (ddName.exists(_.isLetter))
          buf.append(ddName)
        i += len
      } else i += 32*1024
    }

    val ddList = buf.toList
    CloudLogging.stdout(s"""JOBNAME: $jobName
                           |JOBID: $jobId
                           |PROCSTEPNAME: $procStepName
                           |STEPNAME: $stepName
                           |DDs: ${ddList.mkString(",")}
                           |Java Version:
                           |${ZUtil.getJavaVersionInfo}
                           |
                           |JZOS Version:
                           |${ZUtil.getJzosDllVersion}
                           |
                           |JZOS Jar Version:
                           |${ZUtil.getJzosJarVersion}
                           |""".stripMargin)
    ddList
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
