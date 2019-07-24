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

package com.google.cloud.bqsh.cmd

import com.google.cloud.bigquery.JobStatistics.LoadStatistics
import com.google.cloud.bigquery._
import com.google.cloud.bqsh._
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.ZFileProvider

object Load extends Command[LoadConfig] with Logging {
  override val name: String = "bq load"
  override val parser: ArgParser[LoadConfig] = LoadOptionParser

  override def run(cfg: LoadConfig, zos: ZFileProvider): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    val bq = BQ.defaultClient(cfg.projectId, cfg.location, creds)
    logger.info("configuring load job")
    val jobConfig = configureLoadJob(cfg)
    logger.info("submitting load job")
    val jobId = JobId.of(s"bq_load_${System.currentTimeMillis()}_${Util.randString(5)}")
    logger.info("submitting load job")
    val job = bq.create(JobInfo.of(jobId, jobConfig))
    val completed = BQ.await(job, jobId, 3600)
    val status = scala.Option(completed.getStatus)

    if (cfg.statsTable.nonEmpty){
      val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.datasetId, cfg.projectId)
      StatsUtil.insertJobStats(cfg.jesJobName, cfg.jesJobDate, job, statsTable, jobType = "query", source = cfg.path.mkString(","), dest = cfg.tablespec)
    }

    val state = status.map(_.getState.toString).getOrElse("UNKNOWN")
    logger.info(s"job ${jobId.getJob} has status $state")
    val err = status.map(_.getError)
    if (err.nonEmpty){
      val msg = err.map(_.getMessage).getOrElse("")
      val rsn = err.map(_.getReason).getOrElse("")
      logger.error(s"BigQuery error: reason: $rsn message: $msg")
      return Result.Failure(msg)
    }
    Result.Success
  }

  def configureLoadJob(cfg: LoadConfig): LoadJobConfiguration = {
    import scala.collection.JavaConverters.seqAsJavaListConverter
    val destinationTable = BQ.resolveTableSpec(cfg.tablespec, cfg.projectId, cfg.datasetId)
    logger.info(s"destination table=${destinationTable.getTable} sourceUris = ${cfg.path.mkString(",")}")
    val b = LoadJobConfiguration
      .newBuilder(destinationTable, cfg.path.asJava)

    if (cfg.schema.nonEmpty)
      b.setSchema(BQ.parseSchema(cfg.schema))

    if (cfg.source_format.nonEmpty) {
      val formatOptions = FormatOptions.of(cfg.source_format) match {
        case x: CsvOptions =>
          val opts = x.toBuilder
            .setAllowJaggedRows(cfg.allow_jagged_rows)
            .setAllowQuotedNewLines(cfg.allow_quoted_newlines)
            .setSkipLeadingRows(cfg.skip_leading_rows)

          if (cfg.encoding.nonEmpty)
            opts.setEncoding(cfg.encoding)

          if (cfg.field_delimiter.nonEmpty)
            opts.setFieldDelimiter(cfg.field_delimiter)

          if (cfg.quote.nonEmpty)
            opts.setQuote(cfg.quote)

          opts.build()

        case x: DatastoreBackupOptions =>
          val opts = x.toBuilder
          if (cfg.projection_fields.nonEmpty)
            opts.setProjectionFields(cfg.projection_fields.asJava)
          opts.build()

        case x: FormatOptions =>
          x
      }

      logger.info(s"setting format options with type ${formatOptions.getType}")
      b.setFormatOptions(formatOptions)

      if (formatOptions.getType == "AVRO")
        b.setUseAvroLogicalTypes(cfg.use_avro_logical_types)
      else if (formatOptions.getType == "CSV" || formatOptions.getType == "NEWLINE_DELIMITED_JSON") {
        b.setAutodetect(cfg.autodetect)
          .setIgnoreUnknownValues(cfg.ignore_unknown_values)

        if (formatOptions.getType == "CSV") {
          if (cfg.null_marker.nonEmpty)
            b.setNullMarker(cfg.null_marker)
        }
      }
    }

    if (cfg.max_bad_records > 0)
      b.setMaxBadRecords(cfg.max_bad_records)

    if (cfg.time_partitioning_type == TimePartitioning.Type.DAY.name() && cfg.time_partitioning_field.nonEmpty){
      val timePartitioning = TimePartitioning
        .newBuilder(TimePartitioning.Type.DAY)
        .setField(cfg.time_partitioning_field)
        .setRequirePartitionFilter(cfg.requirePartitionFilter)

      if (cfg.time_partitioning_expiration > 0)
        timePartitioning.setExpirationMs(cfg.time_partitioning_expiration)

      b.setTimePartitioning(timePartitioning.build())
    }

    if (cfg.replace)
      b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
    else if (cfg.append)
      b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
    else
      b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_EMPTY)

    val schemaUpdateOptions = BQ.parseSchemaUpdateOption(cfg.schema_update_option)
    if (schemaUpdateOptions.size() > 0)
      b.setSchemaUpdateOptions(schemaUpdateOptions)

    if (cfg.destination_kms_key.nonEmpty) {
      val encryption = EncryptionConfiguration.newBuilder()
        .setKmsKeyName(cfg.destination_kms_key)
        .build()
      b.setDestinationEncryptionConfiguration(encryption)
    }

    if (cfg.clusteringFields.nonEmpty){
      val clustering = Clustering.newBuilder()
        .setFields(cfg.clusteringFields.asJava)
        .build()
      b.setClustering(clustering)
    }

    b.build()
  }

}
