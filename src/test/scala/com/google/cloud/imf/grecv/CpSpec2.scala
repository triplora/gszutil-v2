package com.google.cloud.imf.grecv

import java.nio.charset.StandardCharsets

import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.gszutil.io.ZDataSet
import com.google.cloud.gszutil.{CopyBook, RecordSchema, TestUtil}
import com.google.cloud.imf.grecv.client.GRecvClient
import com.google.cloud.imf.grecv.server.GRecvServer
import com.google.cloud.imf.gzos.gen.DataGenUtil
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, Record}
import com.google.cloud.imf.gzos.{Linux, PackedDecimal, Util}
import com.google.cloud.imf.util.{CloudLogging, Services}
import com.google.protobuf.util.JsonFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CpSpec2 extends AnyFlatSpec with BeforeAndAfterAll {
  CloudLogging.configureLogging(debugOverride = true, errorLogs = Seq("io.netty","org.apache","io.grpc"))

  def server(cfg: GRecvConfig): Future[GRecvServer] = Future{
    val s = new GRecvServer(cfg, Services.storageCredentials())
    s.start(block = false)
    s
  }

  val serverCfg = GRecvConfig("127.0.0.1", debug = true)
  private var grecvServer: Future[GRecvServer] = _

  override def beforeAll(): Unit = {
    grecvServer = server(serverCfg)
  }

  override def afterAll(): Unit = {
    grecvServer.foreach(_.shutdown())
  }

  "Cp" should "copy" in {
    val copybook = new String(TestUtil.resource("FLASH_STORE_DEPT.cpy.txt"),
      StandardCharsets.UTF_8)
    val schema = CopyBook(copybook).toRecordBuilder.build
    JsonFormat.printer().print(schema)
    val schemaProvider = RecordSchema(schema)

    val input = new ZDataSet(TestUtil.resource("FLASH_STORE_DEPT_50k.bin"),136, 27880)

    val cfg1 = GsUtilConfig(schemaProvider = Option(schemaProvider),
                           gcsUri = "gs://gszutil-test/FLASH_STORE_DEPT",
                           projectId = "pso-wmt-dl",
                           datasetId = "dataset",
                           testInput = Option(input),
                           parallelism = 1,
                           nConnections = 3,
                           replace = true,
                           remote = true,
                           remoteHost = serverCfg.host,
                           remotePort = serverCfg.port)
    val res = Cp.run(cfg1, Linux)
    assert(res.exitCode == 0)
  }

  "GRecvClient" should "upload" in {
    val copyBook = CopyBook(
      """    01  TEST-LAYOUT-FIVE.
        |        03  COL-A                    PIC S9(9) COMP.
        |        03  COL-B                    PIC S9(4) COMP.
        |        03  COL-C                    PIC S9(4) COMP.
        |        03  COL-D                    PIC X(01).
        |        03  COL-E                    PIC S9(9) COMP.
        |        03  COL-F                    PIC S9(07)V9(2) COMP-3.
        |        03  COL-G                    PIC S9(05)V9(4) COMP-3.
        |        03  COL-H                    PIC S9(9) COMP.
        |        03  COL-I                    PIC S9(9) COMP.
        |        03  COL-J                    PIC S9(4) COMP.
        |        03  COL-K                    PIC S9(16)V9(2) COMP-3.
        |        03  COL-L                    PIC S9(16)V9(2) COMP-3.
        |        03  COL-M                    PIC S9(16)V9(2) COMP-3.
        |""".stripMargin)

    val record = copyBook.toRecordBuilder.build
    val sp = RecordSchema(record)
    val in = DataGenUtil.generatorFor(sp, 100)
    val request = GRecvRequest.newBuilder
      .setSchema(record)
      .setLrecl(in.lRecl)
      .setBlksz(in.blkSize)
      .setBasepath("gs://gszutil-test/prefix")
      .build

    val sendResult = GRecvClient.upload(request, serverCfg.host, serverCfg.port, 1, Util
      .zProvider, in, "gs://gszutil-test/dsn")
    assert(sendResult.exitCode == 0)
  }

  "DataGenUtil" should "generate" in {
    val sp: RecordSchema = {
      val b = Record.newBuilder
        .setEncoding("EBCDIC")
        .setSource(Record.Source.LAYOUT)
      b.addFieldBuilder().setName("STRING_COL")
        .setTyp(Field.FieldType.STRING)
        .setSize(4)
      b.addFieldBuilder().setName("DECIMAL_COL")
        .setTyp(Field.FieldType.DECIMAL)
        .setSize(PackedDecimal.sizeOf(5,2))
        .setPrecision(7)
        .setScale(2)
      b.addFieldBuilder().setName("INTEGER_COL")
        .setTyp(Field.FieldType.INTEGER)
        .setSize(4)
      RecordSchema(b.build)
    }
    val generator = DataGenUtil.generatorFor(sp, 100)
    val cfg = GsUtilConfig(schemaProvider = Option(sp),
      gcsUri = "gs://gszutil-test/GENERATED",
      testInput = Option(generator),
      parallelism = 1,
      nConnections = 2,
      replace = true,
      remote = true,
      remoteHost = serverCfg.host,
      remotePort = serverCfg.port)
    val res = Cp.run(cfg, Linux)
    assert(res.exitCode == 0)
  }
}
