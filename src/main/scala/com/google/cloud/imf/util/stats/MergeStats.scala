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

import com.google.api.services.bigquery.model.{ExplainQueryStage, Job, JobStatistics2, TableReference}
import com.google.cloud.bqsh.BQ
import com.google.cloud.bqsh.BQ.SchemaRowBuilder

import scala.jdk.CollectionConverters.ListHasAsScala

object MergeStats {
  def forJob(j: Job): Option[MergeStats] = {
    if (j.getStatistics == null ||
        j.getStatistics.getQuery == null ||
        j.getConfiguration == null ||
        j.getConfiguration.getQuery == null ||
        j.getStatistics.getQuery.getStatementType != "MERGE" ||
        j.getConfiguration.getQuery.getDestinationTable == null ||
        j.getStatistics.getQuery.getReferencedTables == null)
      return None
    val q = j.getConfiguration.getQuery

    val s: JobStatistics2 = j.getStatistics.getQuery
    val plan: Seq[ExplainQueryStage] = s.getQueryPlan.asScala.toSeq

    val intoTable: TableReference = q.getDestinationTable
    val fromTable: Option[TableReference] = s.getReferencedTables.asScala.find{t =>
      t.getTableId != intoTable.getTableId ||
      t.getDatasetId != intoTable.getDatasetId ||
      t.getProjectId != intoTable.getProjectId
    }

    val rowsBeforeMerge: Long =
      plan.find{x =>
        BQ.readsFrom(x, intoTable) &&
          x.getSteps.asScala.count(_.getKind == "READ") == 1
      }.map{s =>
        if (s.getRecordsRead != null)
          s.getRecordsRead.toLong
        else 0
      }.getOrElse(0)

    val fromTableRows: Option[Long] =
      fromTable.flatMap(t => plan.find(qs => BQ.readsFrom(qs, t))) match {
        case Some(qs) if qs.getName.endsWith(": Join+") =>
          // simple merge without subquery joins with coalesce of dest table
          if (qs.getRecordsRead != null)
            Option(qs.getRecordsRead.toLong - rowsBeforeMerge)
          else None
        case Some(qs) if qs.getName.endsWith(": Input") =>
          // merge with subquery reads merge table directly
          if (qs.getRecordsRead != null)
            Option(qs.getRecordsRead)
          else None
        case _ =>
          None
      }

    val rowsWritten: Long =
      plan.reverse.find{x =>
        x.getName.endsWith(": Output")
      }.map{s =>
        if (s.getRecordsWritten != null)
          s.getRecordsWritten.toLong
        else 0
      }.getOrElse(0)

    val rowsInserted: Long = if (rowsWritten > rowsBeforeMerge) rowsWritten - rowsBeforeMerge else 0
    val rowsDeleted: Long = if (rowsWritten < rowsBeforeMerge) rowsBeforeMerge - rowsWritten else 0
    val rowsAffected: Long = if (s.getNumDmlAffectedRows != null) s.getNumDmlAffectedRows else 0
    val rowsUpdated: Long = rowsAffected - rowsInserted
    val rowsUnmodified: Long = rowsWritten - rowsAffected

    Option(MergeStats(
      fromTable = fromTable.map(BQ.tableSpec).getOrElse(""),
      intoTable = BQ.tableSpec(intoTable),
      fromTableRows = fromTableRows.getOrElse(0),
      intoTableRows = rowsBeforeMerge,
      rowsWritten = rowsWritten,
      rowsAffected = rowsAffected,
      rowsUnmodified = rowsUnmodified,
      rowsInserted = rowsInserted,
      rowsDeleted = rowsDeleted,
      rowsUpdated = rowsUpdated,
    ))
  }

  def put(s: MergeStats, row: SchemaRowBuilder): Unit = {
    row
      .put("destination", s.intoTable)
      .put("source", s.fromTable)
      .put("rows_before_merge", s.intoTableRows)
      .put("rows_read", s.fromTableRows)
      .put("rows_updated", s.rowsUpdated)
      .put("rows_inserted", s.rowsInserted)
      .put("rows_deleted", s.rowsDeleted)
      .put("rows_unmodified", s.rowsUnmodified)
      .put("rows_affected", s.rowsAffected)
      .put("rows_written", s.rowsWritten)
  }

  def report(s: MergeStats): String = {
    s"""Merge query stats:
       |${s.fromTableRows} rows read from ${s.fromTable}
       |${s.intoTableRows} rows read from ${s.intoTable}
       |${s.rowsWritten} rows output
       |${s.rowsAffected} rows affected
       |${s.rowsUnmodified} rows unmodified
       |${s.rowsInserted} rows inserted
       |${s.rowsDeleted} rows deleted
       |${s.rowsUpdated} rows updated
       |""".stripMargin
  }
}
case class MergeStats(fromTable: String,
                      intoTable: String,
                      intoTableRows: Long,
                      fromTableRows: Long,
                      rowsWritten: Long,
                      rowsAffected: Long,
                      rowsUnmodified: Long,
                      rowsInserted: Long,
                      rowsDeleted: Long,
                      rowsUpdated: Long)
