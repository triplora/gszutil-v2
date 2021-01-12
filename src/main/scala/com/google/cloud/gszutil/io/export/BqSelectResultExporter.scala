package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.{Job, TableResult}
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.Logging

class BqSelectResultExporter(cfg: ExportConfig,
                             zos: MVS,
                             sp: SchemaProvider,
                             exporter: FileExporter = new LocalFileExporter) extends NativeExporter with Logging {

  override def exportData(job: Job): Result = {
    logger.info("Using BqSelectResultExporter.")
    exporter.newExport(MVSFileExport(cfg.outDD, zos))

    val bqResults = job.getQueryResults()
    val totalRowsToExport = bqResults.getTotalRows
    var rowsProcessed: Long = 0

    if (cfg.vartext) {
      logger.info(s"Using pipe-delimited string for export, totalRows=$totalRowsToExport")
      exporter.exportPipeDelimitedRows(bqResults.iterateAll(), totalRowsToExport)
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
