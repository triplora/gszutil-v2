/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
