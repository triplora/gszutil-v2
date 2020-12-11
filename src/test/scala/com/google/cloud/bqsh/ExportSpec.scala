package com.google.cloud.bqsh

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.file.{Files, Paths}

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.{JobId, QueryJobConfiguration}
import com.google.cloud.bqsh.cmd.Export
import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.imf.gzos.{Ebcdic, Linux}
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.util.Services
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.bp.Duration

class ExportSpec extends AnyFlatSpec with BeforeAndAfterAll {

  /**
    * Provide env variables to execute this test
    * PROJECT_ID=pso-wmt-dl
    * OUTFILE=path_to_output_file
    * OUTFILE_LRECL=length
    * OUTFILE_BLKSIZE=blkSIze
    */
  val projectId = sys.env("PROJECT_ID")
  val location = sys.env.getOrElse("LOCATION", "US")

  val zos = Linux

  val sql =
    s"""select *
       |from $projectId.dataset.tbl1
       |limit 200
       |""".stripMargin


  override protected def beforeAll(): Unit = {

    val bq = Services.bigQuery(projectId, location,
      Services.bigqueryCredentials())

    // create a table with one column of each type
    val sql1 =
      s"""
         |create or replace table $projectId.dataset.tbl1 as
         |SELECT
         | 1 as a,
         | 'a' as b,
         | NUMERIC '-3.14'as c,
         | TIMESTAMP '2014-09-27 12:30:00.45-08'as e,
         | CAST(TIMESTAMP '2014-09-27 12:30:00.45-08' AS STRING) as e1,
         | EXTRACT(DATE FROM CURRENT_TIMESTAMP()) as e2,
         | CURRENT_DATE() as f,
         | 123.456e-67 as g
         |""".stripMargin
    val id1 = JobId.newBuilder().setProject(sys.env("PROJECT_ID"))
      .setLocation(sys.env.getOrElse("LOCATION", "US")).setRandomJob().build()
    bq.query(QueryJobConfiguration.newBuilder(sql1)
      .setUseLegacySql(false).build(), id1)
    val job1 = bq.getJob(id1)
    job1.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(2)))
  }


  "Export pipe-delimited file" should "export data to pipe-delimited file" in {
    val cfg = ExportConfig(
      sql = sql,
      projectId = projectId,
      location = location,
      vartext = true)
    Export.run(cfg, zos, Map.empty)

    val values = readStringFromFile().split("\\|")
    assert(values.length == 8)
    assert(values(0) == "1")
    assert(values(1) == "a")
  }

  private def readStringFromFile(): String = {
    val ddPath = Paths.get(System.getenv("OUTFILE"))
    val bytes = Files.readAllBytes(ddPath)
    val decoder = Ebcdic.charset.newDecoder()
    decoder.decode(ByteBuffer.wrap(bytes)).toString
  }
}
