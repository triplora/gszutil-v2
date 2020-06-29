package com.google.cloud.gszutil.io

import com.google.api.services.logging.v2.LoggingScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.gszutil.{RecordSchema, TestUtil}
import com.google.cloud.imf.gzos.gen.{DataGenUtil, DataGenerator}
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.{Ebcdic, Linux}
import com.google.cloud.imf.util.CloudLogging
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec

class CpSpec extends AnyFlatSpec {
  CloudLogging.configureLogging(debugOverride = true)

  val mload1Schema: RecordSchema = {
    val b = Record.newBuilder
      .setVartext(true)
      .setEncoding("EBCDIC")
      .setDelimiter(ByteString.copyFrom("|", Ebcdic.charset))
      .setSource(Record.Source.LAYOUT)

    b.addFieldBuilder().setName("PO_ACTIVITY_ID")
      .setTyp(Field.FieldType.STRING)
      .setSize(10)
    b.addFieldBuilder().setName("PO_NBR")
      .setTyp(Field.FieldType.STRING)
      .setSize(10)
    b.addFieldBuilder().setName("PO_ORDER_DATE")
      .setTyp(Field.FieldType.STRING)
      .setCast(Field.FieldType.DATE)
      .setFormat("YYYYMMDD")
      .setSize(8)
    b.addFieldBuilder().setName("EQUIPMENT_TYPE_CD")
      .setTyp(Field.FieldType.STRING)
      .setSize(1)
    b.addFieldBuilder().setName("EQUIPMENT_ID")
      .setTyp(Field.FieldType.STRING)
      .setSize(10)
    b.addFieldBuilder().setName("SHIPMENT_ID")
      .setTyp(Field.FieldType.INTEGER)
      .setSize(4)
    b.addFieldBuilder().setName("SCAC_CODE")
      .setTyp(Field.FieldType.STRING)
      .setSize(4)
    b.addFieldBuilder().setName("ITEM_NBR")
      .setTyp(Field.FieldType.INTEGER)
      .setSize(4)
    b.addFieldBuilder().setName("UNIT_QTY")
      .setTyp(Field.FieldType.INTEGER)
      .setSize(4)
    b.addFieldBuilder().setName("UNIT_UOM_CODE")
      .setTyp(Field.FieldType.STRING)
      .setSize(2)
    b.addFieldBuilder().setName("LAST_CHANGE_DATE")
      .setTyp(Field.FieldType.STRING)
      .setCast(Field.FieldType.DATE)
      .setFormat("YYYYMMDD")
      .setSize(8)
    b.addFieldBuilder().setName("LAST_CHANGE_TIME")
      .setTyp(Field.FieldType.INTEGER)
      .setSize(4)
    b.addFieldBuilder().setName("LAST_CHANGE_USERID")
      .setTyp(Field.FieldType.STRING)
      .setSize(8)

    RecordSchema(b.build)
  }

  val mloadSchema: RecordSchema =
    RecordSchema(mload1Schema.toRecordBuilder
      .setVartext(false)
      .clearDelimiter()
      .build)

  "Cp" should "copy" in {
    val input = new ZDataSet(TestUtil.resource("mload1.dat"),111, 1110)
    val sp = mloadSchema
    val cfg = GsUtilConfig(schemaProvider = Option(sp),
                           gcsUri = "gs://gszutil-test/mload1.dat",
                           projectId = "pso-wmt-dl",
                           datasetId = "dataset",
                           testInput = Option(input),
                           parallelism = 1,
                           replace = true)
    val res = Cp.run(cfg, Linux)
    assert(res.exitCode == 0)
  }

  it should "generate" in {
    val sp = mloadSchema
    val generator = DataGenUtil.generatorFor(sp)
    System.out.println(generator.generators.zip(sp.decoders).map(_.toString).mkString("\n"))

    val cfg = GsUtilConfig(schemaProvider = Option(sp),
                           gcsUri = "gs://gszutil-test/mload1.gen",
                           projectId = "pso-wmt-dl",
                           datasetId = "dataset",
                           testInput = Option(generator),
                           parallelism = 1,
                           replace = true)
    val res = Cp.run(cfg, Linux)
    assert(res.exitCode == 0)
  }
}
