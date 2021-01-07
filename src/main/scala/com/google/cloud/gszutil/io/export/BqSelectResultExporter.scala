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

    if (cfg.vartext) {
      logger.info("Using pipe-delimited string for export.")
      exporter.exportPipeDelimitedRows(bqResults.iterateAll())
    } else {
      logger.info("Using TD schema for export.")
      exporter.exportBQSelectResult(bqResults.iterateAll(), bqResults.getSchema.getFields, sp.encoders)
    }

    val totalRows = bqResults.getTotalRows
    val rowsWritten = exporter.currentExport.rowsWritten()
    logger.info(s"Received $totalRows rows from BigQuery API, written $rowsWritten rows.")

    require(totalRows == rowsWritten, s"BigQuery API sent $totalRows rows but " +
      s"writer wrote $rowsWritten")

    Result(activityCount = rowsWritten)
  }

  override def close(): Unit = exporter.endIfOpen()
}
