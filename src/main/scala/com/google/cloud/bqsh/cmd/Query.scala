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

import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{BigQueryException, Clustering, JobInfo, QueryJobConfiguration, QueryParameterValue, StandardSQLTypeName, StatsUtil, TimePartitioning}
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh.{ArgParser, BQ, Bqsh, Command, QueryConfig, QueryOptionParser}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.{Logging, Services}

object Query extends Command[QueryConfig] with Logging {
  override val name: String = "bq query"
  override val parser: ArgParser[QueryConfig] = QueryOptionParser

  def run(cfg: QueryConfig, zos: MVS): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    val bq = Services.bigQuery(cfg.projectId, cfg.location, creds)

    val queryString =
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
    logger.info(s"SQL Query:\n$queryString")
    require(queryString.nonEmpty, "query must not be empty")

    val queries =
      if (cfg.allowMultipleQueries) Bqsh.splitSQL(queryString)
      else Seq(queryString)

    var result: Result = null
    for (query <- queries) {
      val jobConfiguration = configureQueryJob(query, cfg, zos)
      val jobId = BQ.genJobId(zos, "query")
      logger.debug(s"generated job id ${jobId.getJob}")

      try {
        logger.info("Submitting query job")
        val job = BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60, cfg.sync)
        if (cfg.sync){
          logger.info("Query job finished")
        }

        // Publish results
        if (cfg.sync && cfg.statsTable.nonEmpty) {
          logger.debug("writing stats")
          val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.projectId, cfg.datasetId)
          logger.debug(s"resolved stats table ${statsTable.getProject}:${statsTable.getDataset}" +
            s".${statsTable.getTable}")
          StatsUtil.insertJobStats(zos, jobId, scala.Option(job), bq,
            statsTable, jobType = "query", dest = cfg.destinationTable)
        }

        // check for errors
        if (cfg.sync) {
          logger.info(s"Checking for errors in job status")
          BQ.getStatus(job) match {
            case Some(status) =>
              logger.info(s"Status = ${status.state}")
              if (status.hasError) {
                val msg = s"Error:\n${status.error}\nExecutionErrors: ${status.executionErrors.mkString("\n")}"
                logger.error(msg)
                result = Result.Failure(msg)
              } else {
                val stats = job.getStatistics[QueryStatistics]
                val conf = job.getConfiguration[QueryJobConfiguration]
                val activityCount: Long =
                  Option[Long](stats.getNumDmlAffectedRows)
                    .getOrElse(Option(bq.getTable(conf.getDestinationTable))
                      .map(_.getNumRows.longValueExact()).getOrElse(0L))
                result = Result(activityCount = activityCount)
              }
              BQ.throwOnError(status)
            case _ =>
              logger.error(s"Job ${jobId.getJob} not found")
              result = Result.Failure("missing status")
          }
        } else {
          result = Result.Success
        }
      } catch {
        case e: BigQueryException =>
          result = Result.Failure(e.getMessage)
      }
    }
    if (result == null) Result.Failure("no queries")
    else result
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

  def configureQueryJob(query: String, cfg: QueryConfig, zos: MVS): QueryJobConfiguration = {
    import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

    val b = QueryJobConfiguration.newBuilder(query)
      .setDryRun(cfg.dryRun)
      .setUseLegacySql(cfg.useLegacySql)
      .setUseQueryCache(cfg.useCache)

    if (cfg.datasetId.nonEmpty)
      b.setDefaultDataset(resolveDataset(cfg.datasetId, cfg.projectId))

    if (cfg.createIfNeeded)
      b.setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)

    if (cfg.useLegacySql)
      b.setAllowLargeResults(cfg.allowLargeResults)

    if (cfg.clusteringFields.nonEmpty){
      val clustering = Clustering.newBuilder
        .setFields(cfg.clusteringFields.asJava)
        .build
      b.setClustering(clustering)
    }

    if (cfg.timePartitioningType == TimePartitioning.Type.DAY.name() && cfg.timePartitioningField.nonEmpty){
      val timePartitioning = TimePartitioning
        .newBuilder(TimePartitioning.Type.DAY)
        .setExpirationMs(cfg.timePartitioningExpiration)
        .setField(cfg.timePartitioningField)
        .setRequirePartitionFilter(cfg.requirePartitionFilter)
        .build()

      b.setTimePartitioning(timePartitioning)
    }

    if (cfg.parameters.nonEmpty){
      val (positionalValues, namedValues) = parseParameters(cfg.parameters)
      if (positionalValues.nonEmpty)
        b.setPositionalParameters(positionalValues.asJava)
      if (namedValues.nonEmpty)
        b.setNamedParameters(namedValues.toMap.asJava)
    }

    val schemaUpdateOptions = BQ.parseSchemaUpdateOption(cfg.schemaUpdateOption)
    if (schemaUpdateOptions.size() > 0)
      b.setSchemaUpdateOptions(schemaUpdateOptions)

    if (cfg.destinationTable.nonEmpty){
      val destinationTable = BQ.resolveTableSpec(cfg.destinationTable, cfg.projectId, cfg.datasetId)
      b.setDestinationTable(destinationTable)
    }

    if (cfg.maximumBytesBilled > 0)
      b.setMaximumBytesBilled(cfg.maximumBytesBilled)

    if (cfg.batch)
      b.setPriority(QueryJobConfiguration.Priority.BATCH)

    if (cfg.replace)
      b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
    else if (cfg.appendTable)
      b.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)

    b.build()
  }
}
