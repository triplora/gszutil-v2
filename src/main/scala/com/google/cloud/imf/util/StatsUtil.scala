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

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.{BigQuery, InsertAllRequest, Job, JobId, LoadJobConfiguration, QueryJobConfiguration, QueryStage, TableId}
import com.google.cloud.bqsh.BQ
import com.google.cloud.imf.gzos.MVS
import com.google.common.collect.ImmutableMap

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsScala}

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

  /** Helper class for extracting information from BigQuery Job statistics
    *
    * @param job BigQuery Query Job instance
    */
  class EnhancedJob(job: Job) {
    def conf: QueryJobConfiguration = job.getConfiguration[QueryJobConfiguration]

    def stats: QueryStatistics = job.getStatistics[QueryStatistics]
    def queryDuration: Long = stats.getEndTime - stats.getStartTime
    def waitDuration: Long = stats.getStartTime - stats.getCreationTime
    def slotMsPerByte: Double = (stats.getTotalSlotMs * 1.0d) / stats.getTotalBytesProcessed
    def slotUtilizationRate: Double = (stats.getTotalSlotMs * 1.0d) / queryDuration

    def isDryRun: Boolean = conf.dryRun
    def bytesProcessed: Long =
      if (isDryRun) stats.getEstimatedBytesProcessed
      else stats.getTotalBytesProcessed

    def isSelect: Boolean = stats.getStatementType == QueryStatistics.StatementType.SELECT
    def selectIntoTable: Option[TableId] =
      if (isSelect) Option(conf.getDestinationTable) else None
    def selectFromTables: Option[List[TableId]] =
      if (isSelect) Option(stats.getReferencedTables.asScala.toList)
      else None

    def isMerge: Boolean = stats.getStatementType == QueryStatistics.StatementType.MERGE
    def mergeIntoTable: Option[TableId] =
      if (isMerge) Option(conf.getDestinationTable) else None
    def mergeFromTable: Option[TableId] =
      if (isMerge)
        tableRefs.filterNot{t => BQ.tablesEqual(t, mergeIntoTable)}.headOption
      else None

    def plan = stats.getQueryPlan.asScala
    def tableRefs = stats.getReferencedTables.asScala

    // number of rows returned by select query
    def selectOutputRows: Option[Long] = {
      if (isSelect) {
        val outputStage = plan.filter(_.getName.endsWith(": Output")).lastOption
        outputStage.map(_.getRecordsWritten)
      } else None
    }

    // number of rows in target table prior to merge query
    def mergeIntoRows: Option[Long] = {
      if (isMerge) {
        val inputStage = plan.find(_.getName.endsWith(": Input"))
        inputStage.map(_.getRecordsRead)
      } else None
    }

    // number of rows in merge from table prior to merge query
    def mergeFromRows: Option[Long] = {
      if (isMerge) {
        val joinStage = plan.find(_.getName.endsWith(": Join+"))
        joinStage.map(_.getRecordsRead)
      } else None
    }

    // number of rows written to target table by merge query
    def mergeOutputRows: Option[Long] = {
      if (isMerge) {
        val outputStage = plan.filter(_.getName.endsWith(": Output")).lastOption
        outputStage.map(_.getRecordsWritten)
      } else None
    }

    // number of new rows added to target table by merge query
    def mergeInsertedRows: Option[Long] = {
      (mergeIntoRows,mergeOutputRows) match {
        case (Some(rowsBefore),Some(rowsAfter)) =>
          Option(rowsAfter - rowsBefore)
        case _ => None
      }
    }

    /** Calculates upper bound on rows updated by merge query
      * joined = inserted + (updated + duplicates)
      * (updated + duplicates) = joined - inserted
      * calculated update count is inflated if duplicates exist
      */
    def mergeUpdatedRows: Option[Long] = {
      (mergeInsertedRows,mergeFromRows) match {
        case (Some(insertedRows),Some(totalRows)) =>
          Option(totalRows - insertedRows)
        case _ => None
      }
    }

    def mergeAffectedRows: Option[Long] = {
      if (isMerge) Option(stats.getNumDmlAffectedRows)
      else None
    }

    /** Print table names and row counts */
    def report: String = {
      val sb = new StringBuilder(1024)
      if (isMerge) {
        val mergeFromTableSpec = BQ.tableSpec(mergeFromTable)
        val mergeIntoTableSpec = BQ.tableSpec(mergeIntoTable)
        val rowsJoined = mergeFromRows.getOrElse(-1)
        val rowsRead = mergeIntoRows.getOrElse(-1)
        val rowsInserted = mergeInsertedRows.getOrElse(-1)
        val rowsUpdated = mergeUpdatedRows.getOrElse(-1)
        val affectedRows = mergeAffectedRows.getOrElse(-1)
        val outputRows = mergeOutputRows.getOrElse(-1)
        sb.append("MERGE query stats:\n")
        sb.append(s"FROM $mergeFromTableSpec ($rowsJoined rows)\n")
        sb.append(s"INTO $mergeIntoTableSpec ($rowsRead rows)\n")
        sb.append(s"Inserted $rowsInserted rows\n")
        sb.append(s"Updated $rowsUpdated rows\n")
        sb.append(s"Output $outputRows rows\n")
        sb.append(s"Affected $affectedRows rows\n")
      } else if (isSelect) {
        val sources = selectFromTables.map(_.map(BQ.tableSpec).mkString("\n     ,")).getOrElse("")
        val dest = BQ.tableSpec(selectIntoTable)
        sb.append("SELECT query stats:\n")
        sb.append(s"FROM $sources\n")
        sb.append(s"INTO $dest\n")
        sb.append(s"Output ${selectOutputRows.getOrElse(-1)}:\n")
      }
      sb.result
    }
  }

  def insertJobStats(zos: MVS, jobId: JobId, job: Option[Job],
                     bq: BigQuery, tableId: TableId, jobType: String = "", source: String = "",
                     dest: String = "", recordsIn: Long = -1, recordsOut: Long = -1): Unit = {
    val row = ImmutableMap.builder[String,Any]()
    row.put("job_name", zos.jobName)
    row.put("job_date", jobDate2Date(zos.jobDate))
    row.put("job_time", jobTime2Time(zos.jobTime))
    row.put("timestamp", epochMillis2Timestamp(System.currentTimeMillis))
    row.put("job_id", jobId.getJob)
    if (jobType.nonEmpty)
      row.put("job_type", jobType)
    if (source.nonEmpty)
      row.put("source", source)
    if (dest.nonEmpty)
      row.put("destination", dest)
    job match {
      case Some(job) =>
        jobType match {
          case "load" =>
            Option(job.getConfiguration[LoadJobConfiguration]) match {
              case Some(value) =>
                Option(job.getStatistics[LoadStatistics])
                  .flatMap(x => Option(x.getOutputRows)) match {
                    case Some(outputRows) =>
                      row.put("records_out", outputRows)
                    case _ =>
                  }
                val cfg = value.toString
                row.put("job_json", cfg)
                logger.debug(s"Job Data:\n$cfg")
              case _ =>
            }

          case "merge_query" =>
            if (recordsIn >= 0)
              row.put("records_in", recordsIn)
            if (recordsOut >= 0)
              row.put("records_out", recordsOut)

          case "select_query" =>
            if (recordsOut >= 0)
              row.put("records_out", recordsOut)

          case "query" =>
            Option(job.getConfiguration[QueryJobConfiguration]) match {
              case Some(value) =>
                Option(job.getStatistics[QueryStatistics])
                  .flatMap(x => Option(x.getNumDmlAffectedRows)) match {
                    case Some(rows) =>
                      row.put("records_out", rows)
                    case _ =>
                  }
                val cfg = value.toString
                row.put("job_json", cfg)
                logger.debug(s"Job Data:\n$cfg")
              case _ =>
            }

          case "cp" =>
            if (recordsIn >= 0)
              row.put("records_in", recordsIn)
            if (recordsOut >= 0)
              row.put("records_out", recordsOut)
          case _ =>
        }
      case _ =>
    }

    logger.debug(s"inserting stats to ${tableId.getProject}:${tableId.getDataset}.${tableId.getTable}")
    bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(jobId.getJob, row.build).build()) match {
      case x if x.hasErrors =>
        val errors = x.getInsertErrors.asScala.values.flatMap(_.asScala).mkString("\n")
        logger.error(s"failed to insert stats for Job ID ${jobId.getJob}\n$errors")
      case _ =>
        logger.debug(s"inserted job stats for Job ID ${jobId.getJob}")
    }
  }

  def insertRow(content: java.util.Map[String,Any],
                bq: BigQuery,
                tableId: TableId): Unit = {
    import scala.jdk.CollectionConverters.MapHasAsScala
    val request = InsertAllRequest.of(tableId, RowToInsert.of(content))
    val result = bq.insertAll(request)
    if (result.hasErrors) {
      import scala.jdk.CollectionConverters.IterableHasAsScala
      val errors = result.getInsertErrors.values.asScala.flatMap(_.asScala).toList
      val tblSpec = s"${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
      val contentStr = content.asScala.toString()
      val sb = new StringBuilder
      sb.append(s"Errors inserting $contentStr into $tblSpec:\n")
      sb.append(errors.map(e => s"${e.getMessage} - ${e.getReason}").mkString("\n"))
      logger.error(sb.result())
    }
  }
}
