package com.google.cloud.imf.grecv

import java.nio.charset.StandardCharsets

import com.google.api.services.logging.v2.LoggingScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.gszutil.io.ZDataSet
import com.google.cloud.gszutil.{CopyBook, RecordSchema, TestUtil}
import com.google.cloud.imf.grecv.grpc.Service
import com.google.cloud.imf.gzos.{Linux, Util}
import com.google.cloud.imf.util.{CloudLogging, Services}
import com.google.protobuf.util.JsonFormat
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CpSpec2 extends AnyFlatSpec {
  CloudLogging.configureLogging(debugOverride = true)

  def server(cfg: GRecvConfig): Future[Service] = Future{
    val s = new Service(cfg, Services.storage())
    s.start(block = false)
    s
  }

  "Cp" should "copy" in {
    val cfg = GRecvConfig("127.0.0.1", tls = false, debug = true, key = "")

    val s = server(cfg)

    val copybook = new String(TestUtil.resource("FLASH_STORE_DEPT.cpy.txt"),
      StandardCharsets.UTF_8)
    val schema = CopyBook(copybook).toRecordBuilder.build
    JsonFormat.printer().print(schema)
    val schemaProvider = RecordSchema(schema)

    val input = new ZDataSet(TestUtil.resource("FLASH_STORE_DEPT_50k.bin"),136, 27880)

    val cfg1 = GsUtilConfig(schemaProvider = Option(schemaProvider),
                           gcsUri = "gs://gszutil-test/v4",
                           projectId = "pso-wmt-dl",
                           datasetId = "dataset",
                           testInput = Option(input),
                           parallelism = 1,
                           nConnections = 3,
                           replace = true,
                           remote = true,
                           remoteHost = "127.0.0.1")
    val res = Cp.run(cfg1, Linux)
    assert(res.exitCode == 0)
  }
}
