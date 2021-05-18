package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.{FieldList, FieldValueList}
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.BinaryEncoder

import scala.jdk.CollectionConverters._

/**
  * Wrapper for FileExporter
  * @todo remove it after FileExporter API refactoring.
  */
class SimpleFileExporterAdapter(fe: FileExporter, cfg: ExportConfig) extends SimpleFileExporter {
  def validateData(schema: FieldList, encoders: Array[BinaryEncoder]): Unit = {
    if (!cfg.vartext) {
      fe.validateExport(schema, encoders)//TODO: currently validation is performed for each file, we actually need to run it only once
    }
  }

  def exportData(rows: Iterable[FieldValueList], schema: FieldList, encoders: Array[BinaryEncoder]): Result = {
    if (cfg.vartext) {
      //zero is passed as rowCount, it is used only for logging so it is safe to ignore it
      fe.exportPipeDelimitedRows(rows.asJava, 0)
    } else {
      fe.exportBQSelectResult(rows.asJava, schema, encoders)
    }
  }

  def getCurrentExporter: FileExport = {
    fe.currentExport
  }

  def endIfOpen(): Unit = fe.endIfOpen()

}
