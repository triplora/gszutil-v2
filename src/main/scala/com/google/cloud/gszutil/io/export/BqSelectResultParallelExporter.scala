package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.`export`.{NativeExporter, PartialPageIterator, SimpleFileExporter}
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.Logging

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._

class BqSelectResultParallelExporter(cfg: ExportConfig,
                                     bq: BigQuery,
                                     jobInfo: GRecvProto.ZOSJobInfo,
                                     sp: SchemaProvider,
                                     exporterFactory: (String, ExportConfig) => SimpleFileExporter) extends NativeExporter(bq, cfg, jobInfo) with Logging {

  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(cfg.workerThreads))

  private var exporters: Seq[SimpleFileExporter] = List()

  override def exportData(job: Job): Result = {
    val jobName = s"Job[id=${job.getJobId.getJob}]"
    if (!job.isDone)
      throw new IllegalArgumentException(s"$jobName still running")
    if (job.getConfiguration == null || !job.getConfiguration.isInstanceOf[QueryJobConfiguration])
      throw new IllegalArgumentException(s"$jobName should have configuration of type QueryJobConfiguration")

    logger.info(s"$jobName Multithreading export started")
    val tableWithResults = bq.getTable(job.getConfiguration.asInstanceOf[QueryJobConfiguration].getDestinationTable)
    val tableSchema = tableWithResults.getDefinition[TableDefinition].getSchema.getFields
    val totalRowsToExport = tableWithResults.getNumRows.intValue()
    val partitionSizePerThread = cfg.partitionSize

    val pageFetcher = (startIndex: Long, pageSize: Long) => {
      val data = bq.listTableData(
        tableWithResults.getTableId,
        TableDataListOption.startIndex(startIndex),
        TableDataListOption.pageSize(pageSize)
      ).getValues.asScala
      Page(data, data.size)
    }
    val partitions = for (leftBound <- 0 to totalRowsToExport by partitionSizePerThread) yield leftBound
    val iterators = partitions.map(leftBound =>
      new PartialPageIterator[Iterable[FieldValueList]](
        startIndex = leftBound,
        endIndex = Math.min(leftBound + partitionSizePerThread, totalRowsToExport),
        pageSize = cfg.partitionPageSize,
        pageFetcher = pageFetcher(_, _))
    )

    exporters = partitions.map(leftBound => exporterFactory("" + (leftBound / partitionSizePerThread), cfg))

    val results = iterators.zip(exporters).map {
      case (iterator, exporter) =>
        Future {
          val partitionName = s"$jobName Batch[${iterator.startIndex}:${iterator.endIndex}]"
          exporter.validateData(tableSchema, sp.encoders)

          var rowsProcessed: Long = 0
          val totalPartitionRows = iterator.endIndex - iterator.startIndex

          while (iterator.hasNext()) {
            exporter.exportData(iterator.next(), tableSchema, sp.encoders) match {
              case Result(_, 0, rowsWritten, _) =>
                rowsProcessed = rowsProcessed + rowsWritten
                logger.info(s"$partitionName $rowsProcessed rows of $totalPartitionRows already exported")
                if (rowsProcessed > totalPartitionRows)
                  throw new IllegalStateException(s"$partitionName Internal issue, to many rows exported!!!")
              case Result(_, 1, _, msg) => throw new IllegalStateException(s"$partitionName Failed when encoding values to file: $msg")
            }
          }
          Result(activityCount = rowsProcessed)
        }
    }.map(Await.result(_, Duration.create(60, TimeUnit.MINUTES)))

    val rowsProcessed = results.map(_.activityCount).sum
    logger.info(s"$jobName Received $totalRowsToExport rows from BigQuery API, written $rowsProcessed rows.")
    require(totalRowsToExport == rowsProcessed, s"$jobName BigQuery API sent $totalRowsToExport rows but " +
      s"writer wrote $rowsProcessed")
    Result(activityCount = rowsProcessed)
  }

  override def close(): Unit = exporters.foreach(_.endIfOpen())
}
