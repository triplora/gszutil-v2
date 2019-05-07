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

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.oauth2.StaticAccessTokenProvider
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery._
import com.google.cloud.gszutil.{Config, Util}
import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.GSXML.CredentialProvider
import org.apache.spark.network.protocol.Encoders
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.threeten.bp.Duration


object BQLoad {
  def run(c: Config, cp: CredentialProvider): Unit = {
    StaticAccessTokenProvider.setCredentialProvider(cp)

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("GSZUtil")
      .config(StaticAccessTokenProvider.sparkConf())
      .getOrCreate()

    val orcUri = s"gs://${c.bq.bucket}/${c.bq.prefix}.orc"

    val copyBook = CopyBook(Util.readS("sku_dly_pos.cpy"))

    import spark.implicits._
    val df: DataFrame = spark.sparkContext
      .makeRDD(Seq(c.inDD), 1)
      .toDS()
      .flatMap(copyBook.reader.readDD)(RowEncoder(copyBook.getSchema))
      .toDF(copyBook.getFieldNames:_*)

    System.out.println(s"Writing ORC to $orcUri")
    df.write
      .mode(SaveMode.Overwrite)
      .option("orc.compress", "none")
      .option("orc.stripe.size", "100000000")
      .orc(orcUri)
    System.out.println(s"Finished Writing ORC")

    System.out.println(s"Creating BigQuery client")
    val bq: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bq.location)
      .setCredentials(cp.getCredentials)
      .setProjectId(c.bq.project)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "GSZUtil 0.1"))
      .build()
      .getService

    System.out.println(s"Loading ORC")
    load(bq, c.bq.table, c, s"$orcUri/part*")
  }

  def load(bq: BigQuery, table: String, c: Config, sourceUri: String, formatOptions: FormatOptions = FormatOptions.orc()): Unit = {

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
    System.out.println(s"Job returned with JobStatus:\n$jobStatus")
  }

  def isValidTableName(s: String): Boolean = {
    s.matches("[a-zA-Z0-9_]{1,1024}")
  }

  def isValidBigQueryName(s: String): Boolean =
    s.matches("[a-zA-Z0-9_]{1,1024}")
}
