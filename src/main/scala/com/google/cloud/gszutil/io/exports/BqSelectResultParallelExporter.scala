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
  //currently there is a limit how much files could be joined by GCS Api, which is 32
  //See https://cloud.google.com/storage/docs/composite-objects
  //also it is more memory efficient to have a lot of small partitions due to releasing of memory on .close()
  private val partitionCount = 32
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(cfg.exporterThreadCount))
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
    val minPartitionSize = 10 * 1024 * 1024 / sp.LRECL //about 10 mb or records
    val partitionSizePerThread = math.max(minPartitionSize, totalRowsToExport / partitionCount + 1)
    //page size in rows for paginate in scope of one partition,
    //will be automatically limited to 10mb in case set value is to big.
    //https://cloud.google.com/bigquery/docs/paging-results
    val partitionPageSize = ((10 * 1024 * 1024) / sp.LRECL * 1.2).toLong // + 20% to be sure that page size bigger then 10 MB
    val partitions = for (leftBound <- 0 to totalRowsToExport by partitionSizePerThread) yield leftBound
    logger.info(
      s"""$jobName Multithreading settings(
         |  threadsCount=${cfg.exporterThreadCount},
         |  partitionCount=${partitions.size},
         |  partitionSize=$partitionSizePerThread,
         |  partitionPageSize=$partitionPageSize)""".stripMargin)

    val pageFetcher = (startIndex: Long, pageSize: Long) => {
      val result = bq.listTableData(
        tableWithResults.getTableId,
        TableDataListOption.startIndex(startIndex),
        TableDataListOption.pageSize(pageSize)
      )
      Page(result.getValues, Iterators.size(result.getValues.iterator()))
    }
    val iterators = partitions.map(leftBound =>
      new PartialPageIterator[java.lang.Iterable[FieldValueList]](
        startIndex = leftBound,
        endIndex = Math.min(leftBound + partitionSizePerThread, totalRowsToExport),
        pageSize = partitionPageSize,
        pageFetcher = pageFetcher(_, _))
    )

    exporters = partitions.map(leftBound => {
      exporterFactory((leftBound / partitionSizePerThread).toString, cfg)
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

  override def close(): Unit = exporters.foreach(_.endIfOpen())
}
