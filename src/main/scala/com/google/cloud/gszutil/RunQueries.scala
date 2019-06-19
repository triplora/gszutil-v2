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

package com.google.cloud.gszutil

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{Job, JobConfiguration, JobId, JobInfo, QueryJobConfiguration, TableId}
import com.google.cloud.gszutil.Util.Logging
import com.ibm.jzos.CrossPlatform

object RunQueries extends Logging {
  val TableSpecParam = "{{ tablespec }}"

  def run(c: Config, creds: GoogleCredentials): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, creds)
    val statements = CrossPlatform.readDDString(c.inDD)
    val jobNamePrefix = s"gszutil_query_${System.currentTimeMillis() / 1000}_${Util.randString(4)}_"

    val queries = split(statements)
    var last: Option[TableId] = None
    for (i <- queries.indices) {
      val query = queries(i)
      if (query.contains(TableSpecParam)) {
        require(last.isDefined)
      }
      val resolvedQuery = query.replaceAllLiterally(TableSpecParam, getTableSpec(last))

      val cfg = queryJobInfo(resolvedQuery)
      val jobId = JobId.of(s"$jobNamePrefix$i")
      logger.debug("Running query\n" + resolvedQuery)
      val job = BQ.runJob(bq, cfg, jobId, 60 * 60 * 2)
      last = getDestTable(job)
    }
  }

  def printTableId(x: TableId): String = {
    s"${x.getProject}.${x.getDataset}.${x.getTable}"
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
