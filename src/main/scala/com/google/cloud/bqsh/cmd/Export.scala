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

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.{BigQueryReadClient, CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession}
import com.google.cloud.bigquery.{BigQuery, BigQueryException, JobInfo, QueryJobConfiguration}
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh.{ArgParser, BQ, Command, ExportConfig, ExportOptionParser}
import com.google.cloud.gszutil.io.{BQBinaryExporter, BQExporter, Exporter, ZRecordWriterT}
import com.google.cloud.gszutil.{CopyBook, SchemaProvider}
import com.google.cloud.imf.gzos.{Ebcdic, MVS, MVSStorage}
import com.google.cloud.imf.util.{Logging, Services, StatsUtil}
import org.apache.avro.Schema

object Export extends Command[ExportConfig] with Logging {
  override val name: String = "bq export"
  override val parser: ArgParser[ExportConfig] = ExportOptionParser

  override def run(cfg: ExportConfig, zos: MVS, env: Map[String,String]): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    logger.info(s"Starting bq export\n$cfg")
    logger.info("Initializing BigQuery client")
    val bq: BigQuery = Services.bigQuery(cfg.projectId, cfg.location, creds)
    logger.info("Initializing BigQuery storage client")
    val bqStorage: BigQueryReadClient = Services.bigQueryStorage(creds)

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
    if (query.isEmpty) {
      val msg = "Empty export query"
      logger.error(msg)
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
            logger.error(msg)
          }
          logger.info(s"Job Status = ${status.state}")
          BQ.throwOnError(job, status)
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
          logger.error(msg)
          throw new RuntimeException(msg)
      }

      // count output rows
      val rowsInDestTable: Long = destTable.getNumRows.longValueExact

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

      var rowsReceived: Long = 0
      val recordWriter: ZRecordWriterT = zos.writeDD(cfg.outDD)
      val exporter: Exporter = if (cfg.vartext)
        new BQExporter(schema, 0, recordWriter, Ebcdic)
      else {
        val sp: SchemaProvider =
          if (cfg.cobDsn.nonEmpty) {
            logger.info(s"reading copybook from DSN=${cfg.cobDsn}")
            CopyBook(zos.readDSNLines(MVSStorage.parseDSN(cfg.cobDsn)).mkString("\n"))
          } else {
            logger.info(s"reading copybook from DD:COPYBOOK")
            zos.loadCopyBook("COPYBOOK")
          }
        BQBinaryExporter(schema, sp, 0, recordWriter, Ebcdic)
      }

      bqStorage.readRowsCallable.call(readRowsRequest).forEach { res =>
        if (res.hasAvroRows)
          rowsReceived += exporter.processRows(res.getAvroRows)
      }
      logger.info(s"Received $rowsReceived rows from BigQuery Storage API ReadStream")
      exporter.close()
      val rowsWritten = recordWriter.count()
      logger.info(s"Finished writing $rowsWritten rows from BigQuery Storage API ReadStream")

      // Publish results
      if (cfg.statsTable.nonEmpty) {
        val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.projectId, cfg.datasetId)
        val tblspec = s"${statsTable.getProject}:${statsTable.getDataset}.${statsTable.getTable}"
        logger.debug(s"Writing stats to $tblspec")
        StatsUtil.insertJobStats(zos, jobId, bq, statsTable, jobType =
          "export", recordsOut = rowsWritten)
      }
      require(rowsReceived == rowsWritten, s"BigQuery Storage API sent $rowsReceived rows but " +
        s"writer wrote $rowsWritten")
      require(rowsInDestTable == rowsWritten, s"Table contains $rowsInDestTable rows but " +
        s" writer wrote $rowsWritten")
      require(rowsInDestTable == rowsReceived, s"Table contains $rowsInDestTable rows but BigQuery " +
        s"Storage API sent $rowsReceived")
      Result(activityCount = rowsWritten)
    } catch {
      case e: BigQueryException =>
        val msg = "export query failed with BigQueryException: " + e.getMessage + "\n"
        logger.error(msg, e)
        Result.Failure(msg)
    }
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
