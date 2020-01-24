package com.google.cloud.bqsh.cmd

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.nio.CharBuffer

import com.google.cloud.bqsh.{ArgParser, Command, JCLUtilConfig, JCLUtilOptionParser}
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.{Exec, MvsJobSubmitter, PdsDirectory, RcException, RecordReader, RecordWriter, ZFile, ZFileConstants, ZFileProvider, ZUtil}

import scala.collection.mutable.ArrayBuffer

object JCLUtil extends Command[JCLUtilConfig] with Logging {
  override val name: String = "jclutil"
  override val parser: ArgParser[JCLUtilConfig] = JCLUtilOptionParser

  override def run(config: JCLUtilConfig, zos: ZFileProvider): Result = {
    val transform: (String) => String = replaceFirst2(_, "BQ")
    val members = new PDSIterator(config.src)
    while (members.hasNext){
      val member = members.next()
      System.out.println(member)
      val newName = transform(member.name)
      val src = s"//'${config.src}(${member.name})'"
      val dest = s"//'${config.dest}($newName)'"
      val result = copy(src, dest, config.limit)
      if (result.exitCode != 0)
        return result
    }
    Result.Success
  }

  def replaceFirst2(name: String, sub: String): String = {
    val arr = name.toCharArray
    arr(0) = sub.charAt(0)
    arr(1) = sub.charAt(1)
    new String(arr)
  }

  def copy(src: String, dest: String, limit: Int): Result = {
    System.out.println(s"$src -> $dest")
    if (ZFile.exists(src)) {
      val in = RecordReader.newReader(src, ZFileConstants.MODE_FLAG_READ)
      if (!ZFile.exists(dest)) {
        val outOpts = s"recfm=${in.getRecfm},lrecl=${in.getLrecl}"
        logger.info(s"Creating ZFile($dest) with options $outOpts")
        val outfile = new ZFile(dest, s"wt,type=record,noseek,$outOpts")
        outfile.close()
        logger.info(s"Opening RecordWriter $dest")
        val out = RecordWriter.newWriter(dest, ZFileConstants.MODE_FLAG_WRITE)
        val buf = new Array[Byte](in.getLrecl)
        var n = 0
        var count = 0
        while (n > -1 && count < limit) {
          n = in.read(buf)
          if (n > -1) {
            out.write(buf)
            count += 1
          }
        }
        in.close()
        out.flush()
        out.close()
        logger.info(s"Copied $count lines from $src to $dest")
        if (count >= limit) Result.Failure(s"$src exceeded line limit")
        else Result.Success
      } else {
        val msg = s"Error: $dest already exists"
        logger.error(msg)
        System.err.println(msg)
        Result.Failure(msg)
      }
    } else {
      val msg = s"Error: $src doesn't exist"
      logger.error(msg)
      System.err.println(msg)
      Result.Failure(msg)
    }
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
    private val dir = new PdsDirectory(pdsName)
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
