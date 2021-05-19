package com.google.cloud.gszutil.io.exports

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1._
import com.google.cloud.bigquery.{BigQuery, Job, QueryJobConfiguration}
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.bqsh.{BQ, ExportConfig}
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.{BQBinaryExporter, BQExporter, Exporter}
import com.google.cloud.imf.gzos.Ebcdic
import com.google.cloud.imf.gzos.pb.GRecvProto
import org.apache.avro.Schema

class BqStorageApiExporter(cfg: ExportConfig,
                           bqStorage: BigQueryReadClient,
                           bq: BigQuery,
                           fileExportFunc: => FileExport,
                           jobInfo: GRecvProto.ZOSJobInfo,
                           sp: SchemaProvider) extends NativeExporter(bq, cfg, jobInfo) {

  private var exporter: Exporter = _
  override def exportData(job: Job): Result = {
    logger.info("Using BqStorageApiExporter.")
    val conf = job.getConfiguration[QueryJobConfiguration]
    val jobId = BQ.genJobId(cfg.projectId, cfg.location, jobInfo, "query")

    val destTable = Option(bq.getTable(conf.getDestinationTable)) match {
      case Some(t) =>
        t
      case None =>
        val msg = s"Destination table ${conf.getDestinationTable.getProject}." +
          s"${conf.getDestinationTable.getDataset}." +
          s"${conf.getDestinationTable.getTable} not found for export job ${BQ.toStr(jobId)}"
        logger.error(msg)
        throw new RuntimeException(msg)
    }

    // count output rows
    val rowsInDestTable: Long = destTable.getNumRows.longValueExact

    val projectPath = s"projects/${cfg.projectId}"
    val tablePath = s"projects/${cfg.projectId}/datasets/" +
      s"${conf.getDestinationTable.getDataset}/tables/${conf.getDestinationTable.getTable}"
    logger.debug("before readSession")
    val session: ReadSession = bqStorage.createReadSession(
      CreateReadSessionRequest.newBuilder
        .setParent(projectPath)
        .setMaxStreamCount(1)
        .setReadSession(ReadSession.newBuilder
          .setTable(tablePath)
          .setDataFormat(DataFormat.AVRO)
          .setReadOptions(TableReadOptions.newBuilder.build)
          .build)
        .build)

    logger.info(s"before parse avro schema, streams count = ${session.getStreamsCount}, $projectPath, $tablePath")
    val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)
    val readRowsRequest = ReadRowsRequest.newBuilder
      .setReadStream(session.getStreams(0).getName)
      .build

    logger.debug(s"Used ${session.getStreamsCount} streams.")
    val recordWriter = fileExportFunc
    exporter = if (cfg.vartext)
      new BQExporter(schema, 0, recordWriter, Ebcdic)
    else {
      BQBinaryExporter(schema, sp, 0, recordWriter, Ebcdic)
    }

    var rowsReceived: Long = 0
    bqStorage.readRowsCallable.call(readRowsRequest).forEach { res =>
      if (res.hasAvroRows)
        rowsReceived += exporter.processRows(res.getAvroRows)
    }
    logger.info(s"Received $rowsReceived rows from BigQuery Storage API ReadStream")
    val rowsWritten = recordWriter.rowsWritten
    exporter.close()

    logger.info(s"Finished writing $rowsWritten rows from BigQuery Storage API ReadStream")

    require(rowsReceived == rowsWritten, s"BigQuery Storage API sent $rowsReceived rows but " +
      s"writer wrote $rowsWritten")
    require(rowsInDestTable == rowsWritten, s"Table contains $rowsInDestTable rows but " +
      s" writer wrote $rowsWritten")
    require(rowsInDestTable == rowsReceived, s"Table contains $rowsInDestTable rows but BigQuery " +
      s"Storage API sent $rowsReceived")

    Result(activityCount = rowsWritten)
  }

  override def close(): Unit = {
    if(exporter != null) exporter.close()
  }
}
