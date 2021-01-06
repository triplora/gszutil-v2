/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.imf.util.stats

import com.google.api.services.bigquery.model.Job
import com.google.cloud.bqsh.BQ
import com.google.cloud.bqsh.BQ.SchemaRowBuilder

import scala.jdk.CollectionConverters.ListHasAsScala

object SelectStats {
  def forJob(j: Job): Option[SelectStats] = {
    if (j.getStatistics == null ||
        j.getStatistics.getQuery == null ||
        j.getStatistics.getQuery.getStatementType != "SELECT" ||
        j.getStatistics.getQuery.getQueryPlan == null ||
        j.getConfiguration.getQuery == null
    ) return None

    val q = j.getConfiguration.getQuery
    val s = j.getStatistics.getQuery
    val plan = s.getQueryPlan.asScala
    val outputStage = plan.filter(_.getName.endsWith(": Output")).lastOption
    val rowsWritten: Long =
      outputStage.map{s =>
        if (s.getRecordsWritten != null)
          s.getRecordsWritten.toLong
        else 0
      }.getOrElse(0)

    val inputs = plan.flatMap{s =>
      s.getSteps.asScala.flatMap{step =>
        val isRead = step.getKind == "READ"
        if (isRead && step.getSubsteps.size > 0){
          val lastSubStep = step.getSubsteps.asScala.last
          val isFrom = lastSubStep.startsWith("FROM")
          val table = lastSubStep.stripPrefix("FROM ")
          if (isFrom && !table.startsWith("__"))
            Option((table, s.getRecordsRead))
          else None
        }
        else None
      }
    }

    val rowsRead = inputs.foldLeft(0L){(a,b) => a + b._2}

    val sources: String = inputs.map{x => s"${x._1}:${x._2}"}.mkString(",")

    val destTable =
      if (q.getDestinationTable != null)
        BQ.tableSpec(q.getDestinationTable)
      else ""

    Option(SelectStats(
      sources = sources,
      destination = destTable,
      rowsRead = rowsRead,
      rowsWritten = rowsWritten))
  }

  def put(s: SelectStats, row: SchemaRowBuilder): Unit = {
    row
      .put("source",s.sources)
      .put("destination",s.destination)
      .put("rows_read",s.rowsRead)
      .put("rows_written",s.rowsWritten)
  }

  def report(s: SelectStats): String = {
    s"""Select results:
       |Wrote ${s.rowsWritten} rows to ${s.destination}
       |Inputs:
       |${s.sources}
       |""".stripMargin
  }
}

case class SelectStats(sources: String,
                       destination: String,
                       rowsRead: Long,
                       rowsWritten: Long)
