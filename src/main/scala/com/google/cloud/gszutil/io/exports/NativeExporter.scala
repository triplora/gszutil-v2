package com.google.cloud.gszutil.io.exports

import com.google.cloud.bigquery._
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.bqsh.{BQ, ExportConfig}
import com.google.cloud.imf.gzos.pb.GRecvProto
import com.google.cloud.imf.util.{Logging, ServiceLogger}

abstract class NativeExporter(bq: BigQuery,
                              cfg: ExportConfig,
                              jobInfo: GRecvProto.ZOSJobInfo)(implicit log: ServiceLogger) extends Logging {

  def close(): Unit
  def exportData(job: Job): Result

  def doExport(query: String): Result = {
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
      log.info(s"Submitting QueryJob.\njobId=${BQ.toStr(jobId)}")
      val job = BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60, sync = true)
      log.info("QueryJob finished.")

      // check for errors
      BQ.getStatus(job) match {
        case Some(status) =>
          if (status.hasError) {
            val msg = s"Error:\n${status.error}\nExecutionErrors: ${status.executionErrors.mkString("\n")}"
            log.error(msg)
          }
          log.info(s"Job Status = ${status.state}")
          BQ.throwOnError(job, status)
          job
        case _ =>
          val msg = s"Job ${BQ.toStr(jobId)} not found"
          log.error(msg)
          throw new RuntimeException(msg)
      }
    }
  }
}
