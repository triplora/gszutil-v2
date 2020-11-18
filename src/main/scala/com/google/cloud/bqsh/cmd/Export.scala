/*
 * Copyright 2019 Google LLC All Rights Reserved.
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

import java.io.{PrintWriter, StringWriter}

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.{BigQueryException, JobInfo, QueryJobConfiguration,
  QueryParameterValue, StandardSQLTypeName}
import com.google.cloud.bigquery.storage.v1.{BigQueryReadClient, CreateReadSessionRequest,
  DataFormat, ReadRowsRequest, ReadSession}
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh.{ArgParser, BQ, Command, ExportConfig, ExportOptionParser}
import com.google.cloud.gszutil.io.BQExporter
import com.google.cloud.imf.gzos.{Ebcdic, MVS}
import com.google.cloud.imf.util.{CloudLogging, Logging, Services, StatsUtil}
import org.apache.avro.Schema

object Export extends Command[ExportConfig] with Logging {
  override val name: String = "bq export"
  override val parser: ArgParser[ExportConfig] = ExportOptionParser

  def run(cfg: ExportConfig, zos: MVS): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    CloudLogging.stdout(s"Initializing BigQuery client\n" +
      s"projectId=${cfg.projectId} location=${cfg.location}")
    val bq = Services.bigQuery(cfg.projectId, cfg.location, creds)
    val bqStorage = BigQueryReadClient.create()

    val query =
      if (cfg.sql.nonEmpty) cfg.sql
      else {
        cfg.dsn match {
          case Some(dsn) =>
            logger.info(s"Reading query from DSN: $dsn")
            zos.readDSNLines(dsn).mkString(" ")
          case None =>
            logger.info("Reading query from DD: QUERY")
            zos.readDDString("QUERY", " ")
        }
      }
    logger.info(s"SQL Query:\n$query")
    if (query.nonEmpty) {
      val msg = "Empty export query"
      CloudLogging.stderr(msg)
      return Result.Failure(msg)
    }

    val jobConfiguration = configureExportQueryJob(query, cfg)
    val jobId = BQ.genJobId(cfg.projectId, cfg.location, zos, "query")

    try {
      logger.info(s"Submitting QueryJob.\njobId=${BQ.toStr(jobId)}")
      val job = BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60, sync = true)
      logger.info(s"QueryJob finished.")

      val conf = job.getConfiguration[QueryJobConfiguration]

      // check for errors
      BQ.getStatus(job) match {
        case Some(status) =>
          if (status.hasError) {
            val msg = s"Error:\n${status.error}\nExecutionErrors: ${status.executionErrors.mkString("\n")}"
            CloudLogging.stderr(msg)
            logger.error(msg)
          }
          logger.info(s"Job Status = ${status.state}")
          BQ.throwOnError(status)
        case _ =>
          val msg = s"Job ${BQ.toStr(jobId)} not found"
          logger.error(msg)
          throw new RuntimeException(msg)
      }
      val destTable = Option(bq.getTable(conf.getDestinationTable)) match {
        case Some(t) =>
          t
        case None =>
          val msg = s"Destination table ${conf.getDestinationTable.getProject}." +
            s"${conf.getDestinationTable.getDataset}." +
            s"${conf.getDestinationTable.getTable} not found for export job ${BQ.toStr(jobId)}"
          CloudLogging.stderr(msg)
          logger.error(msg)
          throw new RuntimeException(msg)
      }

      // count output rows
      val activityCount: Long = destTable.getNumRows.longValueExact

      val projectPath = s"projects/${cfg.projectId}"
      val tablePath = s"projects/${cfg.projectId}/datasets/" +
        s"${conf.getDestinationTable.getDataset}/tables/${conf.getDestinationTable.getTable}"
      val session: ReadSession = bqStorage.createReadSession(
        CreateReadSessionRequest.newBuilder
          .setParent(projectPath)
          .setMaxStreamCount(1)
          .setReadSession(ReadSession.newBuilder
            .setTable(tablePath)
            .setDataFormat(DataFormat.AVRO)
            .setReadOptions(TableReadOptions.newBuilder.build)
            .build).build)

      val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)
      val readRowsRequest = ReadRowsRequest.newBuilder
        .setReadStream(session.getStreams(0).getName)
        .build

      var rowCount: Long = 0
      val recordWriter = zos.writeDD(cfg.outDD)
      val exporter = new BQExporter(schema, 0, recordWriter, Ebcdic)

      bqStorage.readRowsCallable.call(readRowsRequest).forEach{res =>
        if (res.hasAvroRows)
          rowCount += exporter.processRows(res.getAvroRows)
      }
      exporter.close()
      logger.info(s"Finished receiving $rowCount rows from BigQuery Storage API ReadStream")

      // Publish results
      if (cfg.statsTable.nonEmpty) {
        val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.projectId, cfg.datasetId)
        val tblspec = s"${statsTable.getProject}:${statsTable.getDataset}.${statsTable.getTable}"
        logger.debug(s"Writing stats to $tblspec")
        StatsUtil.insertJobStats(zos, jobId, scala.Option(job), bq, statsTable, jobType =
          "export", recordsOut = recordWriter.count())
      }
      require(rowCount == recordWriter.count(), s"BigQuery Storage API sent $rowCount rows but " +
        s"writer wrote ${recordWriter.count()}")
      require(activityCount == recordWriter.count(), s"Table contains $activityCount rows but " +
        s" writer wrote ${recordWriter.count()}")
      require(activityCount == rowCount, s"Table contains $activityCount rows but BigQuery " +
        s"Storage API sent $rowCount")
      Result.Success
    } catch {
      case e: BigQueryException =>
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        val stackTrace = sw.toString
        CloudLogging.stderr(stackTrace)
        val msg = "export query failed with BigQueryException: " + e.getMessage + "\n" + stackTrace
        logger.error(msg)
        Result.Failure(msg)
    }
  }

  def parseParameters(parameters: Seq[String]): (Seq[QueryParameterValue], Seq[(String,QueryParameterValue)]) = {
    val params = parameters.map(_.split(':'))

    val positionalValues = params.flatMap{queryParam =>
      if (queryParam.head.nonEmpty) None
      else {
        val typeId = queryParam(1)
        val value = queryParam(2)
        val typeName =
          if (typeId.nonEmpty) StandardSQLTypeName.valueOf(typeId)
          else StandardSQLTypeName.STRING

        scala.Option(
          QueryParameterValue.newBuilder()
            .setType(typeName)
            .setValue(value)
            .build()
        )
      }
    }

    val namedValues = params.flatMap{x =>
      if (x.head.isEmpty) None
      else {
        val name = x(0)
        val t = x(1)
        val value = x(2)
        val typeName =
          if (t.nonEmpty) StandardSQLTypeName.valueOf(t)
          else StandardSQLTypeName.STRING

        val parameterValue = QueryParameterValue.newBuilder()
          .setType(typeName)
          .setValue(value)
          .build()

        scala.Option((name, parameterValue))
      }
    }

    (positionalValues, namedValues)
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
}
