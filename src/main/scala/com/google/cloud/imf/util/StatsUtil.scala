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

package com.google.cloud.imf.util

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.google.cloud.bigquery.{BigQuery, JobId, TableId}
import com.google.cloud.bqsh.BQ
import com.google.cloud.bqsh.BQ.SchemaRowBuilder
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.stats.{JobStats, LoadStats, LogTable, MergeStats, QueryStats, SelectStats}

object StatsUtil extends Logging {
  private def sdf(f: String): SimpleDateFormat = {
    val simpleDateFormat = new SimpleDateFormat(f)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    simpleDateFormat
  }

  val JobDateFormat: SimpleDateFormat = sdf("yyMMdd")
  val JobTimeFormat: SimpleDateFormat = sdf("HHmmss")
  private val TimestampFormat: SimpleDateFormat = sdf("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
  private val DateFormat: SimpleDateFormat = sdf("yyyy-MM-dd")
  private val TimeFormat: SimpleDateFormat = sdf("HH:mm:ss.SSSSSS")

  def epochMillis2Timestamp(t: Long): String = TimestampFormat.format(new Date(t))
  private def jobDate2Date(jobDate: String): String = DateFormat.format(JobDateFormat.parse(jobDate))
  private def jobTime2Time(jobTime: String): String = TimeFormat.format(JobTimeFormat.parse(jobTime))

  def insertJobStats(zos: MVS, jobId: JobId,
                     bq: BigQuery, tableId: TableId, jobType: String = "", source: String = "",
                     dest: String = "", recordsIn: Long = -1, recordsOut: Long = -1): Unit = {
    val schema = BQ.checkTableDef(bq, tableId, LogTable.schema)
    val row = SchemaRowBuilder(schema)

    row
      .put("job_name", zos.jobName)
      .put("job_date", jobDate2Date(zos.jobDate))
      .put("job_time", jobTime2Time(zos.jobTime))
      .put("timestamp", epochMillis2Timestamp(System.currentTimeMillis))
      .put("job_id", BQ.toStr(jobId))
      .put("job_type", jobType)
      .put("source", source)
      .put("destination", dest)

    if (jobType == "cp" || jobType == "export") {
      if (recordsIn >= 0)
        row.put("rows_read", recordsIn)
      if (recordsOut >= 0)
        row.put("rows_written", recordsOut)
    } else {
      // any BigQuery job
      val bqApi = Services.bigQueryApi(zos.getCredentialProvider().getCredentials)
      val j = BQ.apiGetJob(bqApi, jobId)
      val sb = new StringBuilder
      JobStats.forJob(j).foreach{s => JobStats.put(s,row); sb.append(JobStats.report(s))}
      QueryStats.forJob(j).foreach{s => QueryStats.put(s,row); sb.append(QueryStats.report(s))}
      SelectStats.forJob(j).foreach{s => SelectStats.put(s,row); sb.append(SelectStats.report(s))}
      LoadStats.forJob(j).foreach{s => LoadStats.put(s,row); sb.append(LoadStats.report(s))}
      MergeStats.forJob(j).foreach{s => MergeStats.put(s,row); sb.append(MergeStats.report(s))}
      logger.info(s"Job Statistics:\n${sb.result}")
    }

    logger.info(s"Inserting stats into ${BQ.tableSpec(tableId)}")
    val res = row.insert(bq, tableId, BQ.toStr(jobId))
    BQ.logInsertErrors(res)
  }
}
