package com.google.cloud.bqsh

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.{JobId, QueryJobConfiguration}
import com.google.cloud.bqsh.cmd.Export
import com.google.cloud.imf.grecv.GRecvConfig
import com.google.cloud.imf.grecv.server.GRecvServer
import com.google.cloud.imf.gzos.Linux
import com.google.cloud.imf.util.{CloudLogging, Services}
import com.google.cloud.storage.BlobId
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.bp.Duration

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ExportRemoteSpec extends AnyFlatSpec with BeforeAndAfterAll {

  /**
   * For this test required following ENV variables:
   * BUCKET=GCS bucket
   * PROJECT=GCS project
   * COPYBOOK=absolute path to exportCopybook.txt
   */
  val TestBucket = sys.env("BUCKET")
  val TestProject = sys.env("PROJECT")
  val Location = sys.env.getOrElse("LOCATION", "US")

  CloudLogging.configureLogging(debugOverride = true, errorLogs = Seq("io.netty","org.apache","io.grpc"))

  val bqFunc = (project: String, location: String) => Services.bigQuery(project, location, Services.bigqueryCredentials())
  def server(cfg: GRecvConfig): Future[GRecvServer] = Future{
    val s = new GRecvServer(cfg, Services.storage(), Services.storageApi(Services.storageCredentials()), bqFunc)
    s.start(block = false)
    s
  }

  val serverCfg = GRecvConfig("127.0.0.1", debug = true)
  private var grecvServer: Future[GRecvServer] = _

  val sql =
    s"""select *
       |from $TestProject.dataset.tbl1
       |limit 200
       |""".stripMargin

  val Copybook =
    """
      |01  TEST-LAYOUT-FIVE.
      |        03  A                    PIC S9(4) COMP.
      |        03  B                    PIC X(8).
      |        03  C                    PIC S9(3)V99 COMP-3.
      |""".stripMargin

  val outFile = "export.bin"
  val gcs = Services.storage()
  val zos = Linux

  override def beforeAll(): Unit = {
    grecvServer = server(serverCfg)

    val bq = Services.bigQuery(TestProject, "US",
      Services.bigqueryCredentials())

    val sql1 =
      s"""create or replace table $TestProject.dataset.tbl1 as
         |SELECT
         | 1 as a,
         | 'a' as b,
         | NUMERIC '-3.14'as c
         |""".stripMargin
    val id1 = JobId.newBuilder().setProject(TestProject).setLocation(Location).setRandomJob().build()
    bq.query(QueryJobConfiguration.newBuilder(sql1)
      .setUseLegacySql(false).build(), id1)
    val job1 = bq.getJob(id1)
    job1.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(2)))
  }

  override def afterAll(): Unit = {
    grecvServer.foreach(_.shutdown())
  }

  "Export binary file" should "export data to binary file" in {
    val cfg = ExportConfig(
      sql = sql,
      projectId = TestProject,
      outDD = outFile,
      location = Location,
      bucket = TestBucket,
      remoteHost = serverCfg.host,
      remotePort = serverCfg.port
    )
    val res = Export.run(cfg, zos, Map.empty)

    assert(res.exitCode == 0)
    assert(res.activityCount == 1)
    assert(gcs.get(BlobId.of(TestBucket, s"EXPORT/$outFile")).exists())
  }
}
