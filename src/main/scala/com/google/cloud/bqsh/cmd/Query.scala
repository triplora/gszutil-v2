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

import com.google.cloud.bigquery.{BigQueryException, Clustering, Job, JobId, JobInfo, JobStatistics, QueryJobConfiguration, QueryParameterValue, StandardSQLTypeName, TimePartitioning}
import com.google.cloud.bqsh.{ArgParser, BQ, Bqsh, Command, QueryConfig, QueryOptionParser}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.stats.{JobStats, LoadStats, MergeStats, QueryStats, SelectStats}
import com.google.cloud.imf.util.{GoogleApiL2Retrier, Logging, Services, StatsUtil}

object Query extends Command[QueryConfig] with GoogleApiL2Retrier with Logging {
  override val name: String = "bq query"
  override val parser: ArgParser[QueryConfig] = QueryOptionParser

  override val retriesCount: Int = sys.env.get(s"BQ_QUERY_CONCURRENT_UPDATE_RETRY_COUNT").flatMap(_.toIntOption).getOrElse(5)
  override val retriesTimeoutMillis: Int = sys.env.get(s"BQ_QUERY_CONCURRENT_UPDATE_RETRY_TIMEOUT_SECONDS").flatMap(_.toIntOption).getOrElse(2) * 1000
  val canRetryBqJob = (j: Job) =>
    BQ.getStatus(j).flatMap(_.error).flatMap(_.message).exists(m => m.trim.startsWith("Could not serialize access to table"))

  override def run(cfg: QueryConfig, zos: MVS, env: Map[String,String]): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    logger.info(s"Initializing BigQuery client\n" +
      s"projectId=${cfg.projectId} location=${cfg.location}")
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
      val jobConfiguration = configureQueryJob(query, cfg)

      try {
        var jobId: JobId = null
        def submitJob(): Job = {
          jobId = BQ.genJobId(cfg.projectId, cfg.location, zos, "query")
          logger.info(s"Submitting Query Job\njobid=${BQ.toStr(jobId)}")
          BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60, cfg.sync)
        }
        val job = runWithRetries(submitJob(),"BQ Query fails due to concurrent table update", canRetryBqJob)

        val status = BQ.getStatus(job)
        if (cfg.sync){
          if (status.isDefined) {
            val s = status.get
            if (s.isDone) {
              logger.info("Query job state=DONE")
            } else {
              val msg = s"Query job state=${s.state} but expected DONE"
              logger.error(msg)
            }
          } else {
            val msg = s"Query job status not available"
            logger.error(msg)
          }
        }

        // Publish results
        if (cfg.sync && cfg.statsTable.nonEmpty) {
          val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.projectId, cfg.datasetId)
          logger.info(s"Writing stats to ${BQ.tableSpec(statsTable)}")
          StatsUtil.retryableInsertJobStats(zos, jobId, bq, statsTable, jobType = "query")
        } else {
          val j = BQ.apiGetJob(Services.bigQueryApi(creds), jobId)
          val sb = new StringBuilder
          JobStats.forJob(j).foreach{s => sb.append(JobStats.report(s))}
          QueryStats.forJob(j).foreach{s => sb.append(QueryStats.report(s))}
          SelectStats.forJob(j).foreach{s => sb.append(SelectStats.report(s))}
          LoadStats.forJob(j).foreach{s => sb.append(LoadStats.report(s))}
          MergeStats.forJob(j).foreach{s => sb.append(MergeStats.report(s))}
          logger.info(s"Query Statistics:\n${sb.result}")
        }

        // check for errors
        if (cfg.sync) {
          logger.info(s"Checking for errors in job status")
          status match {
            case Some(status) =>
              logger.info(s"Status = ${status.state}")
              if (status.hasError) {
                val msg = s"Error:\n${status.error}\nExecutionErrors: ${status.executionErrors.mkString("\n")}"
                logger.error(msg)
                result = Result.Failure(msg)
              } else {
                val activityCount: Long =
                  job.getStatistics[JobStatistics] match {
                    case _: JobStatistics.CopyStatistics => -1
                    case _: JobStatistics.ExtractStatistics => -1
                    case s: JobStatistics.LoadStatistics => s.getOutputRows
                    case s: JobStatistics.QueryStatistics =>
                      if (s.getStatementType == "MERGE") s.getNumDmlAffectedRows
                      else if (s.getStatementType == "SELECT") 0
                      else s.getNumDmlAffectedRows
                    case _ =>
                      -1
                  }
                result = Result(activityCount = activityCount)
              }
              BQ.throwOnError(job, status)
            case _ =>
              logger.error(s"Job ${BQ.toStr(jobId)} not found")
              result = Result.Failure("missing status")
          }
        } else {
          result = Result.Success
        }
      } catch {
        case e: BigQueryException =>
          logger.error(s"Query Job threw BigQueryException\nMessage:${e.getMessage}\n" +
            s"Query:\n$query\n")
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

  def configureQueryJob(query: String, cfg: QueryConfig): QueryJobConfiguration = {
    import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

    val b = QueryJobConfiguration.newBuilder(query)
      .setDryRun(cfg.dryRun)
      .setUseLegacySql(cfg.useLegacySql)
      .setUseQueryCache(cfg.useCache)
      .setAllowLargeResults(cfg.allowLargeResults)

    if (cfg.datasetId.nonEmpty)
      b.setDefaultDataset(BQ.resolveDataset(cfg.datasetId, cfg.projectId))

    if (cfg.createIfNeeded)
      b.setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)

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
