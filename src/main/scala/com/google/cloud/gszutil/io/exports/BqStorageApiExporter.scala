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

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class BqStorageApiExporter(cfg: ExportConfig,
                           bqStorage: BigQueryReadClient,
                           bq: BigQuery,
                           fileExportFunc: String => FileExport,
                           jobInfo: GRecvProto.ZOSJobInfo,
                           sp: SchemaProvider) extends NativeExporter(bq, cfg, jobInfo) {

  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(cfg.exporterThreadCount))

  private val MaxStreams = math.max(1, cfg.exporterThreadCount / 3)
  private val exporters = collection.mutable.ListBuffer.empty[Exporter]

  override def exportData(job: Job): Result = {
    logger.info(s"Using BqStorageApiExporter, workersCount=${cfg.exporterThreadCount}")
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

    val rowsInDestTable = destTable.getNumRows.longValueExact
    val projectPath = s"projects/${cfg.projectId}"
    val tablePath = s"projects/${cfg.projectId}/datasets/" +
      s"${conf.getDestinationTable.getDataset}/tables/${conf.getDestinationTable.getTable}"
    val session: ReadSession = bqStorage.createReadSession(
      CreateReadSessionRequest.newBuilder
        .setParent(projectPath)
        .setMaxStreamCount(MaxStreams)
        .setReadSession(ReadSession.newBuilder
          .setTable(tablePath)
          .setDataFormat(DataFormat.AVRO)
          .setReadOptions(TableReadOptions.newBuilder.build)
          .build)
        .build)

    logger.info(s"ReadSession created. RowsInTable=$rowsInDestTable, maxStreamsCount=$MaxStreams, " +
      s"streamsCount=${session.getStreamsCount}, tablePath=$tablePath")
    val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)

    import scala.jdk.CollectionConverters._
    val rowsProcessed = new AtomicLong(0)
    session.getStreamsList.asScala.zipWithIndex.map{streamToIndex =>
      val request =  ReadRowsRequest.newBuilder.setReadStream(streamToIndex._1.getName).build
      val writer = fileExportFunc(streamToIndex._2.toString)
      val exporter = if (cfg.vartext)
        new BQExporter(schema, streamToIndex._2, writer, Ebcdic)
      else {
        BQBinaryExporter(schema, sp, streamToIndex._2, writer, Ebcdic)
      }
      exporters.append(exporter)

      Future {
        bqStorage.readRowsCallable.call(request).forEach { res =>
          if (res.hasAvroRows) {
            val written = exporter.processRows(res.getAvroRows)
            rowsProcessed.addAndGet(written)
          }
        }
        exporter.close()
      }
    }.foreach(Await.result(_, Duration.create(cfg.timeoutMinutes, TimeUnit.MINUTES)))

    val rowsWritten = exporters.map(_.rowsWritten).sum
    logger.info(s"Finished writing $rowsWritten rows from BigQuery Storage API ReadStream")
    require(rowsProcessed.get() == rowsWritten, s"Internal error, rowsWritten doesn't match rowsProcessed.")
    require(rowsInDestTable == rowsWritten, s"Table contains $rowsInDestTable rows but writer wrote $rowsWritten")

    Result(activityCount = rowsWritten)
  }

  override def close(): Unit = exporters.foreach(e => e.close())
}
