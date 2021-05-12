package com.google.cloud.gszutil.io.`export`

import com.google.api.services.storage.StorageScopes
import com.google.cloud.bigquery.{JobId, QueryJobConfiguration}
import com.google.cloud.bqsh.{BQ, ExportConfig}
import com.google.cloud.gszutil.{CopyBook, SchemaProvider}
import com.google.cloud.imf.GRecv.credentials
import com.google.cloud.imf.gzos.Linux
import com.google.cloud.imf.util.Services
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage.ComposeRequest
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.IterableHasAsJava

class BqSelectResultParallelExporterRealBQSpec extends AnyFlatSpec{
  // test was done for debugging of real BQ api with BqSelectResultParallelExporter
  // for performance reasons it is ignored
  it should "read in parallel data from BigQuery" in {
    //some env variables should be set
    assert(sys.env.contains("OUTFILE")) // = OUTFILE
    assert(sys.env.contains("OUTFILE_LRECL")) // = 80
    assert(sys.env.contains("OUTFILE_BLKSIZE")) // = 512

    val zos = Linux
    val projectId = "pso-wmt-td2bq"
    val location = "US"
    val defaultRecordLength = 80

    val bigQuery = Services.bigQuery(projectId, location, Services.bigqueryCredentials())

    val jobId = JobId.newBuilder()
      .setProject(projectId)
      .setLocation(location)
      .setRandomJob()
      .build()

        //~160k records
        val jobCfg = QueryJobConfiguration.newBuilder("SELECT word FROM `bigquery-public-data.samples.shakespeare`")
          .setMaxResults(1)
          .setUseLegacySql(false)
          .build()
        val cfg = ExportConfig(
          vartext = false,
          partitionSize = 50000,
          partitionPageSize = 10000,
          workerThreads = 2
        )

    /*//~9m records, partitionSize to large value for this
    val jobCfg = QueryJobConfiguration.newBuilder("SELECT image_id FROM `bigquery-public-data.open_images.images`")
      .setMaxResults(1)
      .setUseLegacySql(false)
      .build()

    val cfg = ExportConfig(
      // target is ~250mb file
      partitionSize = 250 * 1024 * 1024 / defaultRecordLength,
      // next one will not work as will be limited by 10 mb
      // target is ~75mb memory usage per thread
      partitionPageSize = 75 * 1024 * 1024 / defaultRecordLength,
      workerThreads = 4
    )*/

    val completedJob = BQ.runJob(
      bigQuery, jobCfg, jobId,
      timeoutSeconds = 10 * 60,
      sync = true)

    val schema: SchemaProvider = CopyBook(
      """ 01  TEST-LAYOUT-FIVE.
        |   02  COL-D   PIC X(50).
        |""".stripMargin)

    def exporterFactory(batchId: String, cfg: ExportConfig): SimpleFileExporter = {
      val result = new LocalFileExporter
      result.newExport(new SimpleFileExport("mt_outfile_" + batchId, defaultRecordLength))
      new SimpleFileExporterAdapter(result, cfg)
    }

    //cleanup
    cleanUpTmpFiles()

    // local parallel export
    val multiThreadExporter = new BqSelectResultParallelExporter(cfg, bigQuery, zos.getInfo, schema, exporterFactory)
    multiThreadExporter.exportData(completedJob)
    multiThreadExporter.close()

    var fileNames = Seq.empty[String]
    val gcs = Services.storage(Services.bigqueryCredentials())
    val bucket = "vn51e5b_luminex_multiple-file-write-test"

    def gcsExporterFactory(fileName: String, cfg: ExportConfig): SimpleFileExporter = {
      val result = new LocalFileExporter
      fileNames = fileNames :+ fileName
      result.newExport(GcsFileExport(gcs, "gs://" + bucket + "/" + fileName, defaultRecordLength))
      new SimpleFileExporterAdapter(result, cfg)
    }

    // GCS parallel export
    val gcsMultiThreadExporter = new BqSelectResultParallelExporter(cfg, bigQuery, zos.getInfo, schema, gcsExporterFactory)
    gcsMultiThreadExporter.exportData(completedJob)
    gcsMultiThreadExporter.close()

    val comReq = ComposeRequest.newBuilder()
      .addSource(fileNames.asJava)
      .setTarget(BlobInfo.newBuilder(bucket, "composed-file").build()).build()
    gcs.compose(comReq)

    // local single threded export
    val singleThreadExporter = new BqSelectResultExporter(cfg, bigQuery, zos.getInfo, schema,
      () => new SimpleFileExport("st_outfile_all", defaultRecordLength))
    singleThreadExporter.exportData(completedJob)
    singleThreadExporter.close()


    val chunkNumber = Files.list(Paths.get("./"))
      .map(_.toFile)
      .filter(_.getName.startsWith("mt_outfile_"))
      .count()

    //merge files
    val outFile = new FileOutputStream(new File("mt_outfile_all"))
    for (i <- 0L until chunkNumber) {
      val bytes = Files.readAllBytes(Paths.get("mt_outfile_" + i))
      outFile.write(bytes)
    }
    outFile.close()

    assert(com.google.common.io.Files.equal(new File("mt_outfile_all"), new File("st_outfile_all")))

    cleanUpTmpFiles()
  }

  private def cleanUpTmpFiles(): Unit = {
    Files.list(Paths.get("./"))
      .map(_.toFile)
      .filter(s => s.getName.startsWith("mt_outfile") || s.getName.startsWith("st_outfile") || s.getName.equals("OUTFILE"))
      .forEach(s => s.delete())
  }

  class TestLocalFileExporter(lRecl: Int) extends LocalFileExporter {
    override def newExport(e: FileExport): Unit = {
      e.close()
      super.newExport(new SimpleFileExport("st_outfile_all", lRecl))
    }
  }
}
