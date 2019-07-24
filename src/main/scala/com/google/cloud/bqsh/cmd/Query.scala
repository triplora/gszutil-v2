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

import com.google.cloud.bigquery._
import com.google.cloud.bqsh.BQ.resolveDataset
import com.google.cloud.bqsh._
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.ZFileProvider

object Query extends Command[QueryConfig] with Logging {
  override val name: String = "bq query"
  override val parser: ArgParser[QueryConfig] = QueryOptionParser

  def run(cfg: QueryConfig, zos: ZFileProvider): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    val bq = BQ.defaultClient(cfg.projectId, cfg.location, creds)

    logger.info(s"Reading query from QUERY")
    val queryString = zos.readDDString("QUERY", " ")
    logger.info(s"Read query:\n$queryString")
    require(queryString.nonEmpty, "query must not be empty")

    val queries =
      if (cfg.allowMultipleQueries) Bqsh.splitSQL(queryString)
      else Seq(queryString)

    var result: Result = null
    for (query <- queries){
      val jobConfiguration = configureQueryJob(query, cfg, zos)
      val jobId = JobId.of(cfg.jobId + "_" + Util.randString(5))

      val job = BQ.runJob(bq, jobConfiguration, jobId, cfg.timeoutMinutes * 60)

      // Publish results
      if (cfg.statsTable.nonEmpty){
        val statsTable = BQ.resolveTableSpec(cfg.statsTable, cfg.datasetId, cfg.projectId)
        StatsUtil.insertJobStats(cfg.jesJobName, cfg.jesJobDate, job, statsTable, jobType = "query", dest = cfg.destinationTable)
      }

      BQ.throwOnError(job)
      if (job.getStatus.getState == JobStatus.State.DONE && job.getStatus.getError != null){
        result = Result.Failure(job.getStatus.getError.getMessage)
      } else {
        result = Result.Success
      }
    }
    if (result == null) Result.Failure("no queries")
    else result
  }

  def parseParameters(parameters: Seq[String]): (Seq[QueryParameterValue], Seq[(String,QueryParameterValue)]) = {
    val params = parameters.map(_.split(':'))

    val positionalValues = params.flatMap{x =>
      if (x.head.nonEmpty) None
      else {
        val t = x(1)
        val value = x(2)
        val typeName =
          if (t.nonEmpty) StandardSQLTypeName.valueOf(t)
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

  def configureQueryJob(query: String, cfg: QueryConfig, zos: ZFileProvider): QueryJobConfiguration = {
    import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

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
      val clustering = Clustering.newBuilder()
        .setFields(cfg.clusteringFields.asJava)
        .build()
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
