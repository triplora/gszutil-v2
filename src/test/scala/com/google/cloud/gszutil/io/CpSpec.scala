package com.google.cloud.gszutil.io

import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.gszutil.{RecordSchema, TestUtil}
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.{Ebcdic, Linux}
import com.google.cloud.imf.util.CloudLogging
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec

object CpSpec {
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
}

class CpSpec extends AnyFlatSpec {
  CloudLogging.configureLogging(debugOverride = true, errorLogs = "org.apache"::"io.grpc"::"io.netty"::Nil)
  val TestBucket = sys.env("BUCKET")
  val TestProject = sys.env("PROJECT")

  "Cp" should "copy vartext" in {
    val input = new ZDataSet(TestUtil.resource("mload1.dat"),111, 1110)
    val sp = CpSpec.mload1Schema
    val cfg = GsUtilConfig(schemaProvider = Option(sp),
                           gcsUri = s"gs://$TestBucket/mload1.dat",
                           projectId = TestProject,
                           datasetId = "dataset",
                           testInput = Option(input),
                           parallelism = 1,
                           replace = true)
    val res = Cp.run(cfg, Linux, Map.empty)
    assert(res.exitCode == 0)
  }
}
