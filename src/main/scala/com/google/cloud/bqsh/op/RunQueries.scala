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

package com.google.cloud.bqsh.op

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.{BigQuery, Job, JobConfiguration, JobId, JobInfo, JobStatistics, QueryJobConfiguration, TableId}
import com.google.cloud.bqsh.BQ
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.ZFileProvider

object RunQueries extends Logging {
  def run(source: String, project: String, location: String, dryRun: Boolean, creds: GoogleCredentials, zos: ZFileProvider): Unit = {
    val statements =
      if (zos.ddExists(source))
        zos.readDDString(source)
      else
        zos.readStdin()

    val bq = BQ.defaultClient(project, location, creds)
    runQueries(statements, Map.empty, dryRun, bq)
  }

  def replace(src: String, token: String, value: String): String = {
    src.replaceAllLiterally(s"{{ $token }}", value)
  }

  def replaceAll(src: String, replacements: Iterable[(String,String)]): String = {
    replacements.foldLeft(src){(a,b) => replace(a, b._1, b._2)}
  }

  def runQueries(statements: String, replacements: Map[String,String], dryRun: Boolean, bq: BigQuery): Unit = {
    val jobNamePrefix = s"gszutil_query_${System.currentTimeMillis() / 1000}_${Util.randString(4)}_"

    val queries = split(statements)
    var last: Option[TableId] = None
    for (i <- queries.indices) {
      val resolvedQuery = replaceAll(queries(i), replacements)
      val cfg = queryJobInfo(resolvedQuery, dryRun)
      val jobId = JobId.of(s"$jobNamePrefix$i")
      logger.debug("Running query\n" + resolvedQuery)
      val job = BQ.runJob(bq, cfg, jobId, 60 * 60 * 2)
      val activityCount = getActivityCount(job)
      logger.debug(s"$activityCount rows affected")
      last = getDestTable(job)
    }
  }

  def printTableId(x: TableId): String = {
    s"${x.getProject}.${x.getDataset}.${x.getTable}"
  }

  def getActivityCount(job: Job): Long = {
    job.getStatistics[JobStatistics] match {
      case x: QueryStatistics =>
        x.getNumDmlAffectedRows
      case x: LoadStatistics =>
        x.getOutputRows
      case _ =>
        -1
    }
  }

  def getDestTable(job: Job): Option[TableId] = {
    job.getConfiguration[JobConfiguration] match {
      case j: QueryJobConfiguration =>
        val destTable = Option(j.getDestinationTable)
        destTable.foreach{x =>
          logger.debug(s"Job ${job.getJobId.getJob} wrote to destination table ${printTableId(x)}")
        }
        destTable
      case _ =>
        None
    }
  }

  def getTableSpec(maybeTable: Option[TableId]): String = {
    maybeTable
      .map(printTableId)
      .getOrElse("")
  }

  def queryJobInfo(query: String, dryRun: Boolean): QueryJobConfiguration = {
    QueryJobConfiguration.newBuilder(query)
      .setAllowLargeResults(true)
      .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
      .setDryRun(dryRun)
      .build()
  }

  def split(sql: String): Seq[String] = {
    sql.split(';')
  }
}
