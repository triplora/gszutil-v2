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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery._
import com.google.cloud.gszutil.GSXML.{CredentialProvider, XMLStorage}
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import org.threeten.bp.Duration

import scala.util.Try

object BQLoad {
  def run(c: Config, cp: CredentialProvider): Unit = {
    val gcs: XMLStorage = XMLStorage(cp)

    val data = AvroWriter.create(10)

    System.out.println(s"Writing avro file to gs://${c.bq.bucket}/${c.bq.prefix}")
    val is = new ByteArrayInputStream(data)
    val request = gcs.putObject(c.bq.bucket, c.bq.prefix, is, AvroWriter
      .AvroContentType)
    val response = request.execute()
    if (response.isSuccessStatusCode)
      System.out.println(s"Successfully wrote gs://${c.bq.bucket}/${c.bq.prefix}")
    else
      System.out.println(s"Failed to write gs://${c.bq.bucket}/${c.bq.prefix} (${response.getStatusCode} ${response.getStatusMessage})\n${response.parseAsString}")
    is.close()

    val gcs2: Storage = StorageOptions.newBuilder()
      .setHeaderProvider(FixedHeaderProvider.create("Host", "www.googleapis.com"))
      .setHost("https://restricted.googleapis.com")
      .setProjectId(c.bq.project)
      .build()
      .getService

    val testfile2 = c.bq.prefix + "_jsonapi"
    System.out.println(s"Writing to $testfile2")
    val write2 = Try {
      val w = gcs2.writer(BlobInfo.newBuilder(c.bq.bucket, testfile2).build())
      w.write(ByteBuffer.wrap(data))
      w.close()
    }

    Util.printException(write2)

    System.out.println(s"Writing ORC")
    val write3 = Try {
      OrcWriter.run(c, cp)
    }

    Util.printException(write3)

    System.out.println(s"Creating BigQuery client")
    val bq: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bq.location)
      .setCredentials(cp.getCredentials)
      .setProjectId(c.bq.project)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "GSZUtil 0.1"))
      .build()
      .getService

    System.out.println(s"Loading Avro")
    val sourceUri = s"gs://${c.bq.bucket}/${c.bq.prefix}"
    load(bq, c.bq.table, c, sourceUri, FormatOptions.avro())
    System.out.println(s"Loading ORC")
    load(bq, c.bq.table+"_orc", c, sourceUri+".orc", FormatOptions.orc())

    System.out.println(s"Loading Parquet")
    load(bq, c.bq.table+"_parquet", c, sourceUri+".parquet", FormatOptions.parquet())
  }

  def load(bq: BigQuery, table: String, c: Config, sourceUri: String, formatOptions: FormatOptions) = {

    val jobConf = LoadJobConfiguration
      .newBuilder(TableId.of(c.bq.project, c.bq.dataset, table), sourceUri)
      .setFormatOptions(formatOptions)
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

  def isValidTableName(s: String): Boolean = {
    s.length <= 1024 && s.matches("[a-zA-Z0-9_]")
  }

  def isValidDatasetName(s: String): Boolean = {
    s.length <= 1024 && s.matches("[a-zA-Z0-9_]")
  }
}
