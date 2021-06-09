package com.google.cloud.bqsh

import com.google.cloud.bqsh.cmd.Load
import com.google.cloud.gszutil.TestUtil
import com.google.cloud.imf.gzos.Linux
import com.google.cloud.imf.util.Services
import com.google.cloud.storage.{BlobId, BlobInfo}
import org.scalatest.flatspec.AnyFlatSpec

class LoadITSpec extends AnyFlatSpec {

  val projectId = sys.env("PROJECT_ID")
  val location = sys.env.getOrElse("LOCATION", "US")
  val bucket = sys.env("BUCKET")
  val zos = Linux
  val table = "dataset.loadTestTable"

  it should "load data from ORC file located in Cloud Storage" in {
    //prepare
    val orcName = "loadTestOrc.orc"
    val orcContent = TestUtil.resource("load/loadOrc.orc")
    uploadToStorage(orcContent, orcName)
    //set
    val conf = LoadConfig(
      projectId = projectId,
      location = location,
      tablespec = table,
      path = Seq(s"gs://$bucket/$orcName"),
      replace = true
    )
    val result = Load.run(conf, zos, Map.empty)
    assert(result.exitCode == 0)
  }

  it should "load data with autodetect flag from CSV file located in Cloud Storage" in {
    //prepare
    val csvName = "loadCsv.csv"
    val csvContent = TestUtil.resource("load/loadCsv.csv")
    uploadToStorage(csvContent, csvName)
    //set
    val conf = LoadConfig(
      projectId = projectId,
      location = location,
      tablespec = table,
      path = Seq(s"gs://$bucket/$csvName"),
      replace = true,
      source_format = "CSV",//required to pass
      autodetect = true,//required to pass or instead pass schema
      skip_leading_rows = 1,//required to pass, default is -1
    )
    val result = Load.run(conf, zos, Map.empty)
    assert(result.exitCode == 0)
  }

  it should "load data with schema from CSV file located in Cloud Storage" in {
    //prepare
    val csvName = "loadCsv.csv"
    val csvContent = TestUtil.resource("load/loadCsv.csv")
    uploadToStorage(csvContent, csvName)
    //set
    val conf = LoadConfig(
      projectId = projectId,
      location = location,
      tablespec = table,
      path = Seq(s"gs://$bucket/$csvName"),
      replace = true,
      source_format = "CSV",//required to pass
      schema = Seq("ITEM_NBR:INT64","STORE_NBR:INT64","WM_YR_WK:INT64","VENDOR_NBR:INT64",
        "EPC_MANAGER_NBR:STRING","CASE_REC_QTY:INT64", "CASE_REC_SCAN_QTY:INT64","REC_READ_RATE_PCT:INT64"),
      skip_leading_rows = 1,//required to pass, default is -1
    )
    val result = Load.run(conf, zos, Map.empty)
    assert(result.exitCode == 0)
  }

  def uploadToStorage(bytes: Array[Byte], name: String) = {
    val storage = Services.storage()
    val blobId = BlobId.of(bucket, name)
    storage.create(BlobInfo.newBuilder(blobId).build(), bytes)
  }
}
