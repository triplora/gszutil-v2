package com.google.cloud.bqsh.cmd

import java.io.IOException

import com.google.cloud.bqsh.{ArgParser, Command, JCLUtilConfig, JCLUtilOptionParser}
import com.google.cloud.gszutil.Decoding
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.{Exec, MvsJobSubmitter, PdsDirectory, RcException, RecordReader, RecordWriter,
  ZFile, ZFileConstants, ZFileProvider, ZUtil}

import scala.collection.mutable.ArrayBuffer

object JCLUtil extends Command[JCLUtilConfig] with Logging {
  override val name: String = "jclutil"
  override val parser: ArgParser[JCLUtilConfig] = JCLUtilOptionParser

  override def run(config: JCLUtilConfig, zos: ZFileProvider): Result = {
    val transform: (String) => String = replacePrefix(_, "BQ")
    val members = new PDSIterator(config.src)

    val exprs: Seq[(String,String)] =
      if (config.expressions.nonEmpty)
        config.exprs
      else Seq(
        "BTEQEXT"    -> "BQSHEXT",
        "MLOADEXT"   -> "BQMLDEXT",
        "TPUMP"      -> "BQMLD",
        "TDCMLD"     -> "BQMLD1",
        "//TD"       -> "//BQ",
        "JOBCHK=TD"  -> "JOBCHK=BQ",
        "JOBNAME=TD" -> "JOBNAME=BQ",
        "INCLUDE MEMBER=(TDSP" -> "INCLUDE MEMBER=(BQSP"
      )

    while (members.hasNext){
      val member = members.next()
      val newName = transform(member.name)
      val src = s"//'${config.src}(${member.name})'"
      val dest = s"//'${config.dest}($newName)'"
      val result = copy(src, dest, config.limit, exprs)
      if (result.exitCode != 0) {
        System.out.println(s"Non-zero exit code returned for ${member.name}")
        return result
      }
    }
    Result.Success
  }

  def replacePrefix(name: String, sub: String): String =
    sub + name.substring(sub.length)

  class RecordIterator(val recordReader: RecordReader, limit: Long) extends Iterator[String] with
    AutoCloseable {
    private val buf = new Array[Byte](recordReader.getLrecl)
    private var n = 0
    private var count: Long = 0
    private var closed = false
    override def hasNext: Boolean = n > -1
    override def next(): String = {
      n = recordReader.read(buf)
      if (n > -1 || count < limit) {
        count += 1
        new String(buf,0,n,Decoding.EBCDIC1)
      } else {
        if (count >= limit)
          System.err.println(s"${recordReader.getDsn} exceeded $limit record limit")
        close()
        null
      }
    }
    override def close(): Unit =
      if (!closed) {
        recordReader.close()
        closed = true
      }
  }

  def readWithReplacements(src: String, exprs: Seq[(String,String)], limit: Long): Iterator[String]
  = {
    val in = RecordReader.newReader(src, ZFileConstants.FLAG_DISP_SHR)
    val it = new RecordIterator(in,limit).takeWhile(_ != null)

    // capture trailing 8 characters separately
    val it1 = it.map{line => (line.take(72),line.drop(72))}

    // lines with all replacements applied
    exprs.foldLeft(it1){(a,b) =>
      a.map{x => (x._1.replaceAllLiterally(b._1,b._2), x._2)}
    }.map{x =>
      x._1.take(72) + x._2
    }
  }

  def copy(src: String, dest: String, limit: Int, exprs: Seq[(String,String)]): Result = {
    System.out.println(s"$src -> $dest")
    if (ZFile.exists(dest)) {
      val msg = s"Error: $dest already exists"
      logger.error(msg)
      System.err.println(msg)
      return Result.Failure(msg)
    } else if (!ZFile.exists(src)) {
      val msg = s"Error: $src doesn't exist"
      logger.error(msg)
      System.err.println(msg)
      Result.Failure(msg)
    }

    val it1 = readWithReplacements(src, exprs, 10000)
    logger.info(s"Opening RecordWriter $dest")
    val out = RecordWriter.newWriter(dest, ZFileConstants.FLAG_DISP_SHR)

    var count = 0
    for (record <- it1){
      out.write(record.getBytes)
      count += 1
    }
    out.flush()
    out.close()
    logger.info(s"Copied $count lines from $src to $dest")
    Result.Success
  }

  class PDSMember(val info: PdsDirectory.MemberInfo) {
    def name: String = info.getName
    def lines: Int = info.getStatistics.currentLines
    override def toString: String =
      s"""name:     ${info.getName}
         |created:  ${info.getStatistics.creationDate}
         |modified: ${info.getStatistics.modificationDate}
         |userid:   ${info.getStatistics.userid}
         |version:  ${info.getStatistics.version}
         |lines:    ${info.getStatistics.currentLines}
         |""".stripMargin
  }

  class PDSIterator(val pdsName: String) extends Iterator[PDSMember] {
    private val dir = new PdsDirectory(s"//'$pdsName'")
    import scala.collection.JavaConverters.asScalaIteratorConverter
    private val iter = dir.iterator().asScala

    override def hasNext: Boolean = iter.hasNext
    override def next(): PDSMember = new PDSMember(iter.next()
      .asInstanceOf[PdsDirectory.MemberInfo])
  }

  def submit(jcl: String): Option[MVSJob] = {
    val jobName = jcl.lines.find(_.startsWith("//"))
      .map(s => s.substring(2, s.indexOf(' ')))
    if (jobName.isDefined) {
      val mjs = new MvsJobSubmitter()
      for (line <- jcl.lines)
        mjs.write(line)
      mjs.close()
      val jobId = mjs.getJobid
      Option(new MVSJob(jobName.get, jobId))
    } else None
  }

  def env(): Array[String] = {
    import scala.collection.JavaConverters._
    val p = ZUtil.getEnvironment.asScala
    p.put("_BPX_SHAREAS", "YES")
    p.put("_BPX_SPAWN_SCRIPT", "YES")
    p.map{x => s"${x._1} = ${x._2}"}.toArray
  }

  class MVSJob(jobName: String, jobId: String) {
    def getStatus: String = {
      val cmd = s"jobStatus $jobId"
      val exec = new Exec(cmd, env())
      exec.run()

      val line = Option(exec.readLine())
      if (line.isEmpty)
        throw new IOException("No output from jobStatus child process")

      exec.getStdinWriter.close()

      var lines = 0
      while (Option(exec.readLine()).isDefined) {
        lines +=1
      }

      if (exec.getReturnCode != 0) {
        throw new RcException("jobStatus failed", exec.getReturnCode)
      } else {
        line.getOrElse("")
      }
    }

    def getOutput: String = {
      val cmd = s"jobOutput $jobName $jobId"
      val exec = new Exec(cmd, env())
      exec.run()

      val lines = new ArrayBuffer[String]()
      var line: Option[String] = Option(exec.readLine())
      while (line.isDefined){
        lines.append(line.get)
        line = Option(exec.readLine())
      }

      if (exec.getReturnCode != 0) {
        throw new RcException("jobOutput failed", exec.getReturnCode)
      } else {
        lines.mkString("\n")
      }
    }
  }
}
