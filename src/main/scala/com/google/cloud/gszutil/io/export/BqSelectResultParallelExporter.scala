package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._
import com.google.cloud.bqsh.ExportConfig
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.`export`.{NativeExporter, PartialPageIterator, SimpleFileExporter}
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.{BlobInfo, Storage}
import com.google.cloud.storage.Storage.ComposeRequest

import java.net.URI
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._

class BqSelectResultParallelExporter(cfg: ExportConfig,
                                     bq: BigQuery,
                                     gcs: Storage,
                                     bucketURI: URI,
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
    var filesToCompose = Seq.empty[String]

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

    exporters = partitions.map(leftBound => {
      filesToCompose = filesToCompose :+ (bucketURI.getPath.substring(1) + "_tmp/" + (leftBound / partitionSizePerThread))
      exporterFactory("" + (leftBound / partitionSizePerThread), cfg)
    })

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
                logger.info(s"$partitionName $rowsProcessed rows of $totalPartitionRows already exported by thread ${Thread.currentThread().getName}")
                if (rowsProcessed > totalPartitionRows)
                  throw new IllegalStateException(s"$partitionName Internal issue, to many rows exported!!!")
              case Result(_, 1, _, msg) => throw new IllegalStateException(s"$partitionName Failed when encoding values to file: $msg")
            }
          }
          Result(activityCount = rowsProcessed)
        }
    }.map(Await.result(_, Duration.create(60, TimeUnit.MINUTES)))

    if (exporters.head.getCurrentExporter.isInstanceOf[GcsFileExport]) {
      composeFiles(gcs, filesToCompose, bucketURI)
      deleteFilesAfterCompose(gcs, bucketURI)
    }

    val rowsProcessed = results.map(_.activityCount).sum
    logger.info(s"$jobName Received $totalRowsToExport rows from BigQuery API, written $rowsProcessed rows.")
    require(totalRowsToExport == rowsProcessed, s"$jobName BigQuery API sent $totalRowsToExport rows but " +
      s"writer wrote $rowsProcessed")
    Result(activityCount = rowsProcessed)
  }

  def composeFiles(gcs: Storage, filesToCompose: Seq[String], bucketURI: URI): Unit = {
    logger.info(s"Starting composing ${filesToCompose.size} files")
    val composeStartTime = System.currentTimeMillis()
    val composeRequest = ComposeRequest.newBuilder()
      .addSource(filesToCompose.asJava)
      .setTarget(BlobInfo.newBuilder(bucketURI.getAuthority, bucketURI.getPath.substring(1)).build()).build()
    gcs.compose(composeRequest)
    logger.info(s"Composing completed, " +
      s"time took: ${(System.currentTimeMillis() - composeStartTime) / 1000} seconds.")
  }

  def deleteFilesAfterCompose(gcs: Storage, bucketURI: URI): Unit = {
    val batch =  gcs.batch()
    val filesToDelete = gcs.list(bucketURI.getAuthority,
      Storage.BlobListOption.prefix(bucketURI.getPath.substring(1) + "_tmp"))

    filesToDelete.iterateAll().forEach(file => batch.delete(file.getBlobId))
    batch.submit()
  }

  override def close(): Unit = exporters.foreach(_.endIfOpen())
}
