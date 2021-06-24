package com.google.cloud.imf

import com.google.cloud.bqsh.GsUtilConfig
import com.google.cloud.bqsh.cmd.Cp
import com.google.cloud.gszutil.RecordSchema
import com.google.cloud.imf.gzos.Linux
import com.google.cloud.imf.gzos.gen.DataGenUtil
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field

object GRecvTest {
  def main(args: Array[String]): Unit = {

    val mload1Schema: RecordSchema = {
      val b = Record.newBuilder
        .setEncoding("EBCDIC")
        .setSource(Record.Source.LAYOUT)

      b.addFieldBuilder().setName("PO_ACTIVITY_ID")
        .setTyp(Field.FieldType.STRING)
        .setSize(10)
      b.addFieldBuilder().setName("PO_NBR")
        .setTyp(Field.FieldType.STRING)
        .setSize(10)
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

    val sp = mloadSchema
    val generator = DataGenUtil.generatorFor(sp, sys.env("N").toInt)
    Console.out.println(generator.generators.zip(sp.decoders).map(_.toString).mkString("\n"))

    val cfg = GsUtilConfig(schemaProvider = Option(sp),
      gcsUri = s"gs://${sys.env("BUCKET")}/test/mload1.gen",
      testInput = Option(generator),
      parallelism = 1,
      nConnections = 3,
      replace = true,
      remote = true,
      remoteHost = "127.0.0.1",
      remotePort = 51771)
    val res = Cp.run(cfg, Linux, Map.empty)
    assert(res.exitCode == 0)
  }
}
