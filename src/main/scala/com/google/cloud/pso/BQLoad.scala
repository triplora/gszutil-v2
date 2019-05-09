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

import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.oauth2.StaticAccessTokenProvider
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery
import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.GSXML.CredentialProvider
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZChannel
import com.google.cloud.gszutil.{Config, SHA256, Util}
import com.google.cloud.hadoop.fs.zfile.ZFileSystem
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import com.google.common.hash.Hashing
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.threeten.bp.Duration


object BQLoad extends Logging {

  case class CopyResult(hash: String, duration: Long, bytes: Long)

  def transferWithHash(rc: ZChannel, wc: WritableByteChannel, chunkSize: Int = 4096): CopyResult = {
    val t0 = System.currentTimeMillis
    val buf = ByteBuffer.allocate(chunkSize)
    val buf2 = buf.asReadOnlyBuffer()
    val h = Hashing.sha256().newHasher()
    var i = 0
    while (rc.read(buf) > -1) {
      buf.flip()
      buf2.position(buf.position)
      buf2.limit(buf.limit)
      h.putBytes(buf2)
      wc.write(buf)
      buf.clear()
      if (i % 100 == 0)
        logger.info(s"i=$i")
      i += 1
    }
    rc.close()
    wc.close()
    val t1 = System.currentTimeMillis
    val hash = h.hash().toString
    CopyResult(hash, t1-t0, rc.getBytesRead)
  }

  def copy(gcs: Storage, dd: String, destBucket: String, destPath: String): CopyResult = {
    val rawUri = s"gs://$destBucket/$destPath"
    val in = ZChannel(dd)
    val out = gcs.writer(BlobInfo.newBuilder(BlobId.of(destBucket, destPath)).build())
    logger.info(s"Copying DD://$dd to $rawUri")
    val result = transferWithHash(in, out)
    logger.info(s"Copied ${result.bytes} bytes in ${result.duration} ms (SHA256=${result.hash})")
    result
  }

  def run(c: Config, cp: CredentialProvider): Unit = {
    StaticAccessTokenProvider.setCredentialProvider(cp)
    val conf = ZFileSystem.addToSparkConf(StaticAccessTokenProvider.sparkConf())
      .set("spark.sql.files.maxRecordsPerFile","8888888")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("GSZUtil")
      .config(conf)
      .getOrCreate()

    val orcUri = s"gs://${c.bq.bucket}/${c.bq.prefix}.orc"

    val cpy = "imsku.cpy"
    val input = "zfile://DD/" + c.inDD
    System.out.println(s"Reading from $input with copy book $cpy")
    val copyBook = CopyBook(Util.readS(cpy))
    val df: DataFrame = spark.read
      .format("zfile")
      .schema(copyBook.getSchema)
      .option("copybook", copyBook.raw)
      .load(input)

    System.out.println(s"Writing ORC to $orcUri")
    df.write
      .format("orc")
      .option("orc.compress", "none")
      .mode(SaveMode.Overwrite)
      .save(orcUri)
    System.out.println(s"Finished Writing ORC")

    System.out.println(s"Creating BigQuery client")
    val bq: bigquery.BigQuery = bigquery.BigQueryOptions.newBuilder()
      .setLocation(c.bq.location)
      .setCredentials(cp.getCredentials)
      .setProjectId(c.bq.project)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "GSZUtil 0.1"))
      .build()
      .getService

    System.out.println(s"Loading ORC")
    load(bq, c.bq.table, c, s"$orcUri/part*")
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
