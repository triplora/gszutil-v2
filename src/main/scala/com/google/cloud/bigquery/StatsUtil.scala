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

package com.google.cloud.bigquery

import com.google.cloud.gszutil.Util.Logging
import com.google.common.collect.ImmutableMap


object StatsUtil extends Logging {
  def insertJobStats(jobName: String, jobDate: String, job: Job, tableId: TableId, jobType: String = "", source: String = "", dest: String = ""): Unit = {
    val bq = job.getBigQuery
    val id = job.getJobId.getJob

    val row = ImmutableMap.builder[String,Any]()
    row.put("jobName", jobName)
    row.put("jobDate", jobDate)
    row.put("time", System.currentTimeMillis)
    row.put("jobId", id)
    if (jobType.nonEmpty)
      row.put("jobType", jobType)
    if (source.nonEmpty)
      row.put("source", source)
    if (dest.nonEmpty)
      row.put("destination", dest)
    row.put("jobJSON", job.toPb.toString)

    val request = InsertAllRequest.newBuilder(tableId)
        .addRow(id, row.build)
        .build()
    val response = bq.insertAll(request)
    if (response.hasErrors){
      val id = scala.Option(job.getJobId).flatMap(x => scala.Option(x.getJob)).getOrElse("")
      logger.error(s"failed to insert stats for Job ID $id")
    }
  }
}
