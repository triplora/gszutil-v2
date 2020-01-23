package com.google.cloud.bqsh.cmd

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.CharBuffer

import com.google.cloud.bqsh.{ArgParser, Command, JCLUtilConfig, JCLUtilOptionParser}
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.{FileFactory, PdsDirectory, RecordReader, RecordWriter, ZFile, ZFileConstants, ZFileProvider, ZUtil}

object JCLUtil extends Command[JCLUtilConfig] with Logging {
  override val name: String = "jclutil"
  override val parser: ArgParser[JCLUtilConfig] = JCLUtilOptionParser

  override def run(config: JCLUtilConfig, zos: ZFileProvider): Result = {
    val transform: (String) => String = replaceFirst2(_, "BQ")
    for (member <- new PDSIterator(config.src)){
      System.out.println(member)
      val newName = transform(member.name)
      val src = s"//'${config.src}(${member.name})'"
      val dest = s"//'${config.dest}($newName)'"
      copy(src, dest)
    }
    Result.Success
  }

  def replaceFirst2(name: String, sub: String): String = {
    val arr = name.toCharArray
    arr(0) = sub.charAt(0)
    arr(1) = sub.charAt(1)
    new String(arr)
  }

  def copy(src: String, dest: String): Unit = {
    System.out.println(s"$src -> $dest")
    var count = 0
    if (ZFile.exists(src)) {
      val in = RecordReader.newReader(src, ZFileConstants.MODE_FLAG_READ)
      if (!ZFile.exists(dest)) {
        val outOpts = s"recfm=${in.getRecfm},lrecl=${in.getLrecl}"
        val outfile = new ZFile(dest, s"wt,type=record,noseek,$outOpts")
        outfile.close()
        val out = RecordWriter.newWriter(dest, ZFileConstants.MODE_FLAG_WRITE)
        val buf = new Array[Byte](in.getLrecl)
        var n = 0
        while (n > -1) {
          n = in.read(buf)
          if (n > -1) {
            out.write(buf)
            count += 1
          }
        }
        in.close()
        out.close()
        System.out.println(s"Copied $count lines from $src to $dest")
      } else {
        System.err.println(s"Error: $dest already exists")
      }
    } else {
      System.err.println(s"Error: $src doesn't exist")
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
}
