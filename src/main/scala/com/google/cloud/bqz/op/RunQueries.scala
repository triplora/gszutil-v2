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

package com.google.cloud.bqz.op

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.{Job, JobConfiguration, JobId, JobInfo, JobStatistics, QueryJobConfiguration, TableId}
import com.google.cloud.bqz.BQ
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.{Config, Util}
import com.ibm.jzos.CrossPlatform

object RunQueries extends Logging {
  def run(c: Config, creds: GoogleCredentials): Unit = {
    val statements =
      if (CrossPlatform.ddExists(CrossPlatform.Infile))
        CrossPlatform.readDDString(c.inDD)
      else
        CrossPlatform.readStdin()

    run(c, creds, statements, Map.empty)
  }

  def replace(src: String, token: String, value: String): String = {
    src.replaceAllLiterally(s"{{ $token }}", value)
  }

  def replaceAll(src: String, replacements: Iterable[(String,String)]): String = {
    replacements.foldLeft(src){(a,b) => replace(a, b._1, b._2)}
  }

  def run(c: Config, creds: GoogleCredentials, statements: String, replacements: Map[String,String]): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, creds)

    val jobNamePrefix = s"gszutil_query_${System.currentTimeMillis() / 1000}_${Util.randString(4)}_"

    val queries = split(statements)
    var last: Option[TableId] = None
    for (i <- queries.indices) {
      val resolvedQuery = replaceAll(queries(i), replacements)

      val cfg = queryJobInfo(resolvedQuery)
      val jobId = JobId.of(s"$jobNamePrefix$i")
      logger.debug("Running query\n" + resolvedQuery)
      if (!c.dryRun) {
        val job = BQ.runJob(bq, cfg, jobId, 60 * 60 * 2)
        val activityCount = getActivityCount(job)
        logger.debug(s"$activityCount rows affected")
        last = getDestTable(job)
      } else {
        last = Option(TableId.of("project","dataset","table"))
      }
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

  def queryJobInfo(query: String): QueryJobConfiguration = {
    QueryJobConfiguration.newBuilder(query)
      .setAllowLargeResults(true)
      .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
      .build()
  }

  def split(sql: String): Seq[String] = {
    sql.split(';')
  }
}
