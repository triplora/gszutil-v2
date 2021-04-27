package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.{BigQuery, BigQueryException, Job, JobId, JobInfo, QueryJobConfiguration}
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh.{BQ, ExportConfig}
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.Logging

abstract class NativeExporter(bq: BigQuery,
                              cfg: ExportConfig,
                              jobInfo: GRecvProto.ZOSJobInfo) extends Logging {

  def close(): Unit
  def exportData(job: Job): Result

  def export(query: String): Result = {
    val configuration = configureExportQueryJob(query, cfg)
    val jobId = getJobId(jobInfo, cfg)
    val job = submitExportJob(bq, configuration, jobId, cfg)
    try {
      exportData(job)
    } finally {
      close()
    }
  }

  def configureExportQueryJob(query: String, cfg: ExportConfig): QueryJobConfiguration = {
    val b = QueryJobConfiguration.newBuilder(query)
      .setDryRun(cfg.dryRun)
      .setUseLegacySql(false)
      .setUseQueryCache(cfg.useCache)

    if (cfg.datasetId.nonEmpty)
      b.setDefaultDataset(resolveDataset(cfg.datasetId, cfg.projectId))

    if (cfg.maximumBytesBilled > 0)
      b.setMaximumBytesBilled(cfg.maximumBytesBilled)

    if (cfg.batch)
      b.setPriority(QueryJobConfiguration.Priority.BATCH)

    b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)

    b.build()
  }

  def getJobId(jobInfo: GRecvProto.ZOSJobInfo, cfg: ExportConfig): JobId =
    BQ.genJobId(cfg.projectId, cfg.location, jobInfo, "query")

  def submitExportJob(bq: BigQuery, jobConfiguration: QueryJobConfiguration, jobId: JobId, cfg: ExportConfig): Job = {
    try {
      logger.info(s"Submitting QueryJob.\njobId=${BQ.toStr(jobId)}")
      val job = BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60, sync = true)
      logger.info(s"QueryJob finished.")

      // check for errors
      BQ.getStatus(job) match {
        case Some(status) =>
          if (status.hasError) {
            val msg = s"Error:\n${status.error}\nExecutionErrors: ${status.executionErrors.mkString("\n")}"
            logger.error(msg)
          }
          logger.info(s"Job Status = ${status.state}")
          BQ.throwOnError(job, status)
          job
        case _ =>
          val msg = s"Job ${BQ.toStr(jobId)} not found"
          logger.error(msg)
          throw new RuntimeException(msg)
      }
    }
  }
}
