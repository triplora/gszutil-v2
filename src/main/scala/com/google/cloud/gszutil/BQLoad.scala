/*
 * Copyright 2019 Google LLC
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

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery._
import com.google.cloud.gszutil.GSXML.CredentialProvider
import org.threeten.bp.Duration

object BQLoad {
  def run(c: Config, cp: CredentialProvider): Unit = {
    val bq: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bq.location)
      .setCredentials(cp.getCredentials)
      .setProjectId(c.bq.project)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "GSZUtil 0.1"))
      .build()
      .getService

    val sourceUri = s"gs://${c.bq.bucket}/${c.bq.prefix}"

    val jobConf = LoadJobConfiguration
      .newBuilder(TableId.of(c.bq.project, c.bq.dataset, c.bq.table), sourceUri)
      .setAutodetect(true)
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
      .build()

    val jobInfo = JobInfo
      .newBuilder(jobConf)
      .setJobId(JobId.of(s"gszutil_load_${System.currentTimeMillis()}"))
      .build()

    System.out.println(s"Submitting load job:\n$jobInfo")
    val job = bq.create(jobInfo)

    System.out.println(s"Waiting for job to complete")
    job.waitFor(RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
      RetryOption.totalTimeout(Duration.ofMinutes(1)))

    val jobStatus = job.getStatus
    System.out.println(s"Job returned with JobStatus::\n$jobStatus")
  }
}
