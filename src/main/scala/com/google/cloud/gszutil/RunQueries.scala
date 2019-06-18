package com.google.cloud.gszutil

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.{BigQuery, BigQueryException, Job, JobConfiguration, JobId, JobInfo, QueryJobConfiguration, TableId}
import com.google.cloud.gszutil.Util.Logging
import com.google.common.base.Preconditions
import com.ibm.jzos.CrossPlatform
import org.threeten.bp.Duration

object RunQueries extends Logging {
  val TableSpecParam = "{{ tablespec }}"

  def run(c: Config, creds: GoogleCredentials): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, creds)
    val statements = CrossPlatform.readDDString(c.inDD)
    val jobNamePrefix = s"gszutil_query_${System.currentTimeMillis() / 1000}_${Util.randString(4)}_"

    val queries = split(statements)
    var last: Option[TableId] = None
    for (i <- queries.indices) {
      val query = queries(i)
      if (query.contains(TableSpecParam)) {
        require(last.isDefined)
      }
      val resolvedQuery = query.replaceAllLiterally(TableSpecParam, getTableSpec(last))

      val cfg = queryJobInfo(resolvedQuery)
      val jobId = JobId.of(s"$jobNamePrefix$i")
      logger.debug("Running query\n" + resolvedQuery)
      val job = runJob(bq, cfg, jobId, 60 * 60 * 2)
      last = getDestTable(job)
    }
  }

  def printTableId(x: TableId): String = {
    s"${x.getProject}.${x.getDataset}.${x.getTable}"
  }

  def getDestTable(job: Job): Option[TableId] = {
    job.getConfiguration[JobConfiguration] match {
      case j: QueryJobConfiguration =>
        val destTable = Option(j.getDestinationTable)
        destTable.foreach{x =>
          logger.debug(s"Job ${job.getJobId.getJob} wrote to destination table ${printTableId(x)}")
        }
        destTable
      case _ =>
        None
    }
  }

  def getTableSpec(maybeTable: Option[TableId]): String = {
    maybeTable
      .map(printTableId)
      .getOrElse("")
  }

  def runJob(bq: BigQuery, cfg: QueryJobConfiguration, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(bq)
    Preconditions.checkNotNull(cfg)
    Preconditions.checkNotNull(jobId)
    try {
      val job = bq.create(JobInfo.of(jobId, cfg))
      await(job, jobId, timeoutSeconds)
    } catch {
      case e: BigQueryException =>
        if (e.getMessage.startsWith("Already Exists")) {
          await(bq, jobId, timeoutSeconds)
        } else {
          throw new RuntimeException(e)
        }
    }
  }

  def await(bq: BigQuery, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(jobId)
    val job = bq.getJob(jobId)
    await(job, jobId, timeoutSeconds)
  }

  def await(job: Job, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(jobId)
    if (job == null) {
      throw new RuntimeException(s"Job ${jobId.getJob} doesn't exist")
    } else {
      job.waitFor(RetryOption.totalTimeout(Duration.ofSeconds(timeoutSeconds)))
    }
  }

  def queryJobInfo(query: String): QueryJobConfiguration = {
    QueryJobConfiguration.newBuilder(query)
      .setAllowLargeResults(true)
      .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
      .build()
  }

  def split(sql: String): Seq[String] = {
    sql.split(';')
  }
}
