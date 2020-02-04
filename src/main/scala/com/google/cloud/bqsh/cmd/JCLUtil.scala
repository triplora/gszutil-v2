package com.google.cloud.bqsh.cmd

import com.google.cloud.bqsh.{ArgParser, Command, JCLUtilConfig, JCLUtilOptionParser}
import com.google.cloud.gszutil.Util.{dsn, Logging}
import com.ibm.jzos.ZFileProvider


object JCLUtil extends Command[JCLUtilConfig] with Logging {
  override val name: String = "jclutil"
  override val parser: ArgParser[JCLUtilConfig] = JCLUtilOptionParser

  override def run(config: JCLUtilConfig, zos: ZFileProvider): Result = {
    val transform: (String) => String = replacePrefix(_, "BQ")
    val members = zos.listPDS(config.srcDSN)

    val exprs: Seq[(String,String)] =
      if (config.expressions.nonEmpty)
        config.exprs
      else Seq(
        "BTEQEXT"    -> "BQSHEXT",
        "MLOADEXT"   -> "BQMLDEXT",
        "TPUMP"      -> "BQMLD",
        "TDCMLD"     -> "BQMLD1",
        "FEXPOEXT"   -> "BQXPOEXT",
        "//TD"       -> "//BQ",
        "JOBCHK=TD"  -> "JOBCHK=BQ",
        "JOBNAME=TD" -> "JOBNAME=BQ",
        "INCLUDE MEMBER=(TDSP" -> "INCLUDE MEMBER=(BQSP"
      )

    if (config.filter.nonEmpty){
      System.out.println(s"Filter regex = '${config.filter}'")
    }

    while (members.hasNext){
      val member = members.next()
      if (config.filter.isEmpty || member.name.matches(config.filter)){
        System.out.println(s"Processing '${member.name}'")
        val result = copy(dsn(config.src,member.name),
                          dsn(config.dest,transform(member.name)),
                          config.limit,
                          exprs,
                          zos)
        if (result.exitCode != 0) {
          System.out.println(s"Non-zero exit code returned for ${member.name}")
          return result
        }
      } else {
        System.out.println(s"Ignored '${member.name}'")
      }
    }
    Result.Success
  }

  def replacePrefix(name: String, sub: String): String =
    sub + name.substring(sub.length)

  def readWithReplacements(lines: Iterator[String],
                           exprs: Seq[(String,String)],
                           limit: Long): Iterator[String] = {
    val it = lines.buffered
    val lrecl = it.head.length
    val hasMarginDigits = it.head.takeRight(8).forall(_.isDigit)
    if (lrecl == 80 && hasMarginDigits) {
      // Trailing 8 characters separately
      val it1 = it.map{line => (line.take(72),line.drop(72))}

      // lines with all replacements applied
      exprs.foldLeft(it1){(a,b) =>
        a.map{x => (x._1.replaceAllLiterally(b._1,b._2), x._2)}
      }.map{x =>
        x._1.take(72) + x._2
      }
    } else {
      // Entire record is eligible for replacement
      exprs.foldLeft[Iterator[String]](it){(a,b) =>
        a.map{x => x.replaceAllLiterally(b._1,b._2)}
      }
    }
  }

  def copy(src: String, dest: String, limit: Int, exprs: Seq[(String,String)],
           zos: ZFileProvider): Result = {
    System.out.println(s"$src -> $dest")
    if (zos.exists(dest)) {
      val msg = s"Error: $dest already exists"
      logger.error(msg)
      System.err.println(msg)
      return Result.Failure(msg)
    } else if (!zos.exists(src)) {
      val msg = s"Error: $src doesn't exist"
      logger.error(msg)
      System.err.println(msg)
      Result.Failure(msg)
    }

    val it1 = readWithReplacements(zos.readDSNLines(src), exprs, limit)
    logger.info(s"Opening RecordWriter $dest")
    val out = zos.writeDSN(dest)

    for (record <- it1)
      out.write(record.getBytes)

    out.close()
    logger.info(s"Copied ${out.count} lines from $src to $dest")
    Result.Success
  }
}
