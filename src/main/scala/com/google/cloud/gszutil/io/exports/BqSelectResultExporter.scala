package com.google.cloud.gszutil.io.exports

import com.google.cloud.bigquery.{BigQuery, Job, TableResult}
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.gzos.pb.GRecvProto

class BqSelectResultExporter(cfg: ExportConfig,
                             bq: BigQuery,
                             jobInfo: GRecvProto.ZOSJobInfo,
                             sp: SchemaProvider,
                             fileExportFunc: => FileExport) extends NativeExporter(bq, cfg, jobInfo) {

  val exporter = new LocalFileExporter

  override def exportData(job: Job): Result = {
    logger.info("Using BqSelectResultExporter.")
    exporter.newExport(fileExportFunc)
    val bqResults = job.getQueryResults()
    val totalRowsToExport = bqResults.getTotalRows
    var rowsProcessed: Long = 0

    if (cfg.vartext) {
      logger.info(s"Using pipe-delimited string for export, totalRows=$totalRowsToExport")
      val res = exporter.exportPipeDelimitedRows(bqResults.iterateAll(), totalRowsToExport)
      rowsProcessed = res.activityCount
    } else {
      logger.info(s"Using TD schema for export, totalRows=$totalRowsToExport")
      // bqResults.iterateAll() fails with big amount of data
      // the reason why 'manual' approach is used
      //exporter.exportBQSelectResult(bqResults.iterateAll(), bqResults.getSchema.getFields, sp.encoders)
      //validation
      exporter.validateExport(bqResults.getSchema.getFields, sp.encoders)
      var currentPage: TableResult = bqResults
      // first page should always be present
      var hasNext = true
      while (hasNext) {
        logger.info("Encoding page of data")
        exporter.exportBQSelectResult(currentPage.getValues,
          bqResults.getSchema.getFields, sp.encoders) match {
          // success exitCode = 0
          case Result(_, 0, rowsWritten, _) =>
            rowsProcessed += rowsWritten
            logger.info(s"$rowsWritten rows of current page written")
            logger.info(s"$rowsProcessed rows of $totalRowsToExport already exported")
            if (currentPage.hasNextPage) {
              hasNext = true
              currentPage = currentPage.getNextPage
            }
            else {
              hasNext = false
            }
            if (rowsProcessed > totalRowsToExport)
              throw new RuntimeException("Internal issue, to many rows exported!!!")

          // failure, exitCode = 1
          case Result(_, 1, _, msg) => throw new RuntimeException(s"Failed when encoding values to file: $msg")
        }
      }
    }

    logger.info(s"Received $totalRowsToExport rows from BigQuery API, written $rowsProcessed rows.")
    require(totalRowsToExport == rowsProcessed, s"BigQuery API sent $totalRowsToExport rows but " +
      s"writer wrote $rowsProcessed")
    Result(activityCount = rowsProcessed)
  }

  override def close(): Unit = exporter.endIfOpen()
}
