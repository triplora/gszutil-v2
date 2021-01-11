package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.Job
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
    val totalRows = bqResults.getTotalRows

    if (cfg.vartext) {
      logger.info(s"Using pipe-delimited string for export, totalRows=$totalRows")
      exporter.exportPipeDelimitedRows(bqResults.iterateAll())
    } else {
      logger.info(s"Using TD schema for export, totalRows=$totalRows")
      exporter.exportBQSelectResult(bqResults.iterateAll(), bqResults.getSchema.getFields, sp.encoders)
    }

    val rowsWritten = exporter.currentExport.rowsWritten()
    logger.info(s"Received $totalRows rows from BigQuery API, written $rowsWritten rows.")

    require(totalRows == rowsWritten, s"BigQuery API sent $totalRows rows but " +
      s"writer wrote $rowsWritten")

    Result(activityCount = rowsWritten)
  }

  override def close(): Unit = exporter.endIfOpen()
}
