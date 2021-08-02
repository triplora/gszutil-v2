package com.google.cloud.gszutil.io.exports

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.common.collect.Iterators

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class BqSelectResultParallelExporter(cfg: ExportConfig,
                                     bq: BigQuery,
                                     jobInfo: GRecvProto.ZOSJobInfo,
                                     sp: SchemaProvider,
                                     exporterFactory: (String, ExportConfig) => SimpleFileExporter) extends NativeExporter(bq, cfg, jobInfo) {

  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(cfg.exporterThreadCount))
  private var exporters: Seq[SimpleFileExporter] = List()

  val _10MB = 10 * 1024 * 1024

  override def exportData(job: Job): Result = {
    val jobName = s"Job[id=${job.getJobId.getJob}]"

    logger.info(s"$jobName Multithreading export started")
    val tableWithResults = bq.getTable(job.getConfiguration.asInstanceOf[QueryJobConfiguration].getDestinationTable)
    val tableSchema = tableWithResults.getDefinition[TableDefinition].getSchema.getFields
    val totalRowsToExport = tableWithResults.getNumRows.intValue()

    val pageSize = _10MB / sp.LRECL
    val partitionSize = math.max(pageSize, totalRowsToExport / cfg.exporterThreadCount + 1)
    val partitions = for (leftBound <- 0 to totalRowsToExport by partitionSize) yield leftBound

    logger.info(
      s"""$jobName Multithreading settings(
         |  totalRowsToExport=$totalRowsToExport,
         |  threadsCount=${cfg.exporterThreadCount},
         |  partitionCount=${partitions.size},
         |  partitionSize=$partitionSize)
         |  BQDestinationTable=${tableWithResults.getTableId}""".stripMargin)

    val iterators = partitions.map(leftBound =>
      new PartialPageIterator[java.lang.Iterable[FieldValueList]](
        startIndex = leftBound,
        endIndex = Math.min(leftBound + partitionSize, totalRowsToExport),
        pageSize = pageSize,
        pageFetcher = fetchPage(tableWithResults, _, _))
    )

    exporters = partitions.map(leftBound => {
      exporterFactory((leftBound / partitionSize).toString, cfg)
    })

    exporters.headOption.foreach(_.validateData(tableSchema, sp.encoders))

    val exportedRows = new AtomicLong(0)
    val results = iterators.zip(exporters).map {
      case (iterator, exporter) =>
        Future {
          try {
            val partitionName = s"$jobName Batch[${iterator.startIndex}:${iterator.endIndex}]"

            var rowsProcessed: Long = 0
            val totalPartitionRows = iterator.endIndex - iterator.startIndex

            while (iterator.hasNext()) {
              exporter.exportData(iterator.next(), tableSchema, sp.encoders) match {
                case Result(_, 0, rowsWritten, _) =>
                  rowsProcessed = rowsProcessed + rowsWritten
                  logger.info(s"$partitionName, exported by thread=[${Thread.currentThread().getName}]: [$rowsProcessed / $totalPartitionRows], " +
                    s"total exported: [${exportedRows.addAndGet(rowsWritten)} / $totalRowsToExport]")
                  if (rowsProcessed > totalPartitionRows)
                    throw new IllegalStateException(s"$partitionName Internal issue, to many rows exported!!!")
                case Result(_, 1, _, msg) => throw new IllegalStateException(s"$partitionName Failed when encoding values to file: $msg")
              }
            }
            Result(activityCount = rowsProcessed)
          } finally {
            exporter.endIfOpen()
          }
        }
    }.map(Await.result(_, Duration.create(cfg.timeoutMinutes, TimeUnit.MINUTES)))

    val rowsProcessed = results.map(_.activityCount).sum
    logger.info(s"$jobName Received $totalRowsToExport rows from BigQuery API, written $rowsProcessed rows.")
    require(totalRowsToExport == rowsProcessed, s"$jobName BigQuery API sent $totalRowsToExport rows but " +
      s"writer wrote $rowsProcessed")
    Result(activityCount = rowsProcessed)
  }

  def fetchPage(table: Table, startIndex: Long, pageSize: Long): Page[java.lang.Iterable[FieldValueList]] = {
    val result = bq.listTableData(
      table.getTableId,
      TableDataListOption.startIndex(startIndex),
      TableDataListOption.pageSize(pageSize)
    )
    Page(result.getValues, Iterators.size(result.getValues.iterator()))
  }

  override def close(): Unit = exporters.foreach(_.endIfOpen())
}
