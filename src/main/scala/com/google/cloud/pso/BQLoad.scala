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
package com.google.cloud.pso

import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.gszutil.Util.{CredentialProvider, Logging}
import com.google.cloud.gszutil.orc.WriteORCFile
import com.google.cloud.gszutil.{Config, GCS}
import com.google.cloud.{RetryOption, bigquery}
import com.ibm.jzos.CrossPlatform
import org.threeten.bp.Duration


object BQLoad extends Logging {
  def run(c: Config, cp: CredentialProvider): Unit = {
    WriteORCFile.run(
      gcsUri = s"gs://${c.bq.bucket}/${c.bq.path}",
      in = CrossPlatform.readDD(c.inDD),
      copyBook = CrossPlatform.loadCopyBook(c.copyBookDD),
      gcs = GCS.defaultClient(cp.getCredentials),
      batchSize = c.batchSize,
      timeoutMinutes = 600)
  }

  def load(bq: bigquery.BigQuery, table: String, c: Config, sourceUri: String, formatOptions: bigquery.FormatOptions = bigquery.FormatOptions.orc()): Unit = {

    val jobConf = bigquery.LoadJobConfiguration
      .newBuilder(bigquery.TableId.of(c.bq.project, c.bq.dataset, table), sourceUri)
      .setFormatOptions(formatOptions)
      .setAutodetect(true)
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
      .build()

    val jobInfo = bigquery.JobInfo
      .newBuilder(jobConf)
      .setJobId(bigquery.JobId.of(s"gszutil_load_${System.currentTimeMillis()}"))
      .build()

    System.out.println(s"Submitting load job:\n$jobInfo")
    val job = bq.create(jobInfo)

    System.out.println(s"Waiting for job to complete")
    job.waitFor(RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
      RetryOption.totalTimeout(Duration.ofMinutes(1)))

    val jobStatus = job.getStatus
    System.out.println(s"Job returned with JobStatus:\n$jobStatus")
  }

  def isValidTableName(s: String): Boolean = {
    s.matches("[a-zA-Z0-9_]{1,1024}")
  }

  def isValidBigQueryName(s: String): Boolean =
    s.matches("[a-zA-Z0-9_]{1,1024}")
}
