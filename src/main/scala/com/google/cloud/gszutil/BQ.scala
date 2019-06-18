package com.google.cloud.gszutil

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.{BigQuery, BigQueryException, BigQueryOptions, Job, JobId, JobInfo, QueryJobConfiguration}
import com.google.cloud.http.HttpTransportOptions
import com.google.common.base.Preconditions
import org.threeten.bp.Duration

object BQ {
  def defaultClient(project: String, location: String, credentials: Credentials): BigQuery = {
    BigQueryOptions.newBuilder()
      .setLocation(location)
      .setProjectId(project)
      .setCredentials(credentials)
      .setTransportOptions(HttpTransportOptions.newBuilder()
        .setHttpTransportFactory(new CCATransportFactory)
        .build())
      .setRetrySettings(RetrySettings.newBuilder()
        .setMaxAttempts(8)
        .setTotalTimeout(Duration.ofMinutes(30))
        .setInitialRetryDelay(Duration.ofSeconds(8))
        .setMaxRetryDelay(Duration.ofSeconds(32))
        .setRetryDelayMultiplier(2.0d)
        .build())
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "gszutil-0.1"))
      .build()
      .getService
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
}
