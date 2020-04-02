package com.google.cloud.gszutil.io

import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.gszutil.{RecordSchema, TestUtil, Util}
import com.google.cloud.gzos.Ebcdic
import com.google.cloud.gzos.pb.Schema.{Field, Record}
import com.google.protobuf.ByteString
import com.ibm.jzos.Linux
import org.scalatest.FlatSpec

class CpSpec extends FlatSpec {
  Util.configureLogging()
  "Cp" should "copy" in {
    val b = Record.newBuilder
      .setVartext(true)
      .setEncoding("EBCDIC")
      .setDelimiter(ByteString.copyFrom("|", Ebcdic.charset))
      .setSource(Record.Source.LAYOUT)
      .setLrecl(111)

    b.addFieldBuilder().setName("PO_ACTIVITY_ID")
      .setTyp(Field.FieldType.STRING)
      .setSize(10)
    b.addFieldBuilder().setName("PO_NBR")
      .setTyp(Field.FieldType.STRING)
      .setSize(10)
    b.addFieldBuilder().setName("PO_ORDER_DATE")
      .setTyp(Field.FieldType.DATE)
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
      .setTyp(Field.FieldType.DATE)
      .setFormat("YYYYMMDD")
      .setSize(8)
    b.addFieldBuilder().setName("LAST_CHANGE_TIME")
      .setTyp(Field.FieldType.INTEGER)
      .setSize(4)
    b.addFieldBuilder().setName("LAST_CHANGE_USERID")
      .setTyp(Field.FieldType.STRING)
      .setSize(8)

    val schemaProvider = RecordSchema(b.build)

    val input = new ZDataSet(TestUtil.resource("mload1.dat"),111, 1110)

    val cfg = GsUtilConfig(schemaProvider = Option(schemaProvider),
                           destinationUri = "gs://gszutil-test/v3/test1",
                           projectId = "pso-wmt-dl",
                           datasetId = "dataset",
                           testInput = Option(input),
                           parallelism = 1,
                           replace = true)
    Cp.run(cfg, Linux)
  }
}
