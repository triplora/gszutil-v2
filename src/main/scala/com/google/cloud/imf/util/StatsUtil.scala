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

import scala.collection.mutable
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
    val plan: IndexedSeq[QueryStage] =
      Option(stats.getQueryPlan).map(_.asScala.toIndexedSeq).getOrElse(IndexedSeq.empty)
    val tableRefs: IndexedSeq[TableId] =
      Option(stats.getReferencedTables).map(_.asScala.toIndexedSeq).getOrElse(IndexedSeq.empty)

    def stats: QueryStatistics = job.getStatistics[QueryStatistics]
    val statementType: String = Option(stats.getStatementType).map(_.toString).getOrElse("Query")
    val queryDuration: Long = stats.getEndTime - stats.getStartTime
    val waitDuration: Long = stats.getStartTime - stats.getCreationTime

    val slotMsToTotalBytesRatio: Double =
      if (stats.getTotalBytesProcessed > 0)
        (stats.getTotalSlotMs * 1.0d) / stats.getTotalBytesProcessed
      else 0

    val slotUtilizationRate: Double =
      if (queryDuration > 0)
        (stats.getTotalSlotMs * 1.0d) / queryDuration
      else 0

    val shuffleBytes: Long =
      plan.map(_.getShuffleOutputBytes).sum

    val shuffleBytesSpilled: Long =
      plan.map(_.getShuffleOutputBytesSpilled).sum

    val shuffleBytesToTotalBytesRatio: Double =
      if (stats.getTotalBytesProcessed > 0)
        shuffleBytes / stats.getTotalBytesProcessed
      else 0

    val shuffleSpillToShuffleBytesRatio: Double =
      if (shuffleBytes > 0)
        shuffleBytesSpilled / shuffleBytes
      else 0

    val shuffleSpillToTotalBytesRatio: Double =
      if (stats.getTotalBytesProcessed > 0)
        shuffleBytesSpilled / stats.getTotalBytesProcessed
      else 0

    val stageCount: Int =
      plan.length

    def countSteps(qs: QueryStage): Int =
      Option(qs.getSteps).map(_.size).getOrElse(0)

    def countSubSteps(qss: QueryStage): Int =
      Option(qss.getSteps)
        .map(_.asScala.foldLeft(0){(acc1, step) => countSubSteps(step) + acc1})
        .getOrElse(0)

    def countSubSteps(qss: QueryStage.QueryStep): Int =
      Option(qss.getSubsteps).map(_.size).getOrElse(0)

    val stepCount: Int =
      plan.foldLeft(0){(acc, queryStage) => countSteps(queryStage) + acc}

    val subStepCount: Int =
      plan.foldLeft(0){(acc,stage) => countSubSteps(stage) + acc}

    /** Summary of inputs, outputs and referenced tables
      */
    val stageSummary: IndexedSeq[String] = {
      plan.map{s =>
        val tbls = s.getSteps.asScala.flatMap{s1 =>
          val last = s1.getSubsteps.asScala.last
          val tbl = last.stripPrefix("FROM ")
          if (s1.getName == "READ" && last.startsWith("FROM ") && !tbl.startsWith("__")) {
            Option(tbl)
          } else None
        }
        val inputs = if (tbls.nonEmpty) tbls.mkString(" inputs:",",","") else ""
        s"${s.getName} in:${s.getRecordsRead} out:${s.getRecordsWritten} steps:${countSteps(s)} " +
          s"subSteps:${countSubSteps(s)}$inputs"
      }
    }

    def isDryRun: Boolean = conf.dryRun
    def bytesProcessed: Long =
      if (isDryRun) stats.getEstimatedBytesProcessed
      else stats.getTotalBytesProcessed

    def isSelect: Boolean = statementType == "SELECT"
    def selectIntoTable: Option[TableId] =
      if (isSelect) Option(conf.getDestinationTable) else None
    def selectFromTables: List[TableId] =
      if (isSelect) Option(stats.getReferencedTables).map(_.asScala.toList).getOrElse(Nil)
      else Nil

    def isMerge: Boolean = statementType == "MERGE"

    def mergeIntoTable: Option[TableId] =
      if (isMerge) Option(conf.getDestinationTable) else None

    val mergeFromTable: Option[TableId] =
      if (isMerge)
        tableRefs.filterNot{t => BQ.tablesEqual(t, mergeIntoTable)}.headOption
      else None

    /** Rows read by select query from all inputs */
    val selectInputRows: Option[Seq[(String,Long)]] = {
      if (isSelect) {
        val inputs = plan.flatMap{s =>
          s.getSteps.asScala.flatMap{step =>
            val isRead = step.getName == "READ"
            val lastSubStep = step.getSubsteps.asScala.last
            val isFrom = lastSubStep.startsWith("FROM")
            val table = lastSubStep.stripPrefix("FROM ")
            val fromTable = !table.startsWith("__")
            if (isRead && isFrom && fromTable){
              Option((table,s.getRecordsRead))
            } else None
          }
        }.toList
        Option(inputs)
      } else None
    }

    /** Rows returned by select query */
    val selectOutputRows: Option[Long] = {
      if (isSelect) {
        val outputStage = plan.filter(_.getName.endsWith(": Output")).lastOption
        outputStage.map(_.getRecordsWritten)
      } else None
    }

    def countReads(qs: QueryStage): Int =
      qs.getSteps.asScala.count(_.getName == "READ")

    /** check if a stage reads from specified table */
    def readsFrom(qs: QueryStage, t: TableId): Boolean = {
      val ts = BQ.tableSpec(t)
      qs.getSteps.asScala.exists{ s =>
        s.getName == "READ" && s.getSubsteps.asScala.exists{s1 =>
          s1.endsWith(ts) && s1.startsWith("FROM ")
        }
      }
    }

    /** Rows in target table prior to merge query
      * Used as initial row count to calculate number of inserts
      */
    val mergeIntoRows: Option[Long] = {
      if (isMerge) {
        val mergeIntoStage: Option[QueryStage] =
          mergeIntoTable.flatMap(t => plan.find(qs => readsFrom(qs, t)))

        if (mergeIntoStage.map(qs => countReads(qs)).contains(1))
          mergeIntoStage.map(_.getRecordsRead)
        else None
      } else None
    }

    /** Rows in merge from table prior to merge query
      * May not have duplicate keys unless aggregated by a subquery
      */
    val mergeFromRows: Option[Long] = {
      if (isMerge) {
        val mergeFromStage: Option[QueryStage] =
          mergeFromTable.flatMap(t => plan.find(qs => readsFrom(qs, t)))

        mergeFromStage match {
          case Some(qs) if qs.getName.endsWith(": Join+") =>
            // simple merge without subquery joins with coalesce of dest table
            Option(qs.getRecordsRead - mergeIntoRows.getOrElse(0L))
          case Some(qs) if qs.getName.endsWith(": Input") =>
            // merge with subquery reads merge table directly
            Option(qs.getRecordsRead)
          case _ =>
            None
        }
      } else None
    }

    /** Rows written to target table by merge query
      * This is the number of rows in the target table after the query completes
      */
    val mergeOutputRows: Option[Long] = {
      if (isMerge) {
        val outputStage = plan.filter(_.getName.endsWith(": Output")).lastOption
        outputStage.map(_.getRecordsWritten)
      } else None
    }

    /** Rows added to target table by merge query
      * Calculated by comparing final row count to initial row count
      */
    val mergeInsertedRows: Option[Long] =
      for {before <- mergeIntoRows; after <- mergeOutputRows}
      yield after - before

    /** Affected rows includes both inserted and updated rows
      * Rows not matched by a merge query are not included
      */
    val mergeAffectedRows: Option[Long] =
      if (isMerge) Option(stats.getNumDmlAffectedRows)
      else None

    /** Calculates rows updated by merge query
      */
    val mergeUpdatedRows: Option[Long] =
      for {ins <- mergeInsertedRows; aff <- mergeAffectedRows}
      yield aff - ins

    // format with 2 decimal places
    private def f(x: Double): String = "%1.2f".format(x)

    /** Print table names and row counts */
    def report: String = {
      val gbProcessed = stats.getTotalBytesProcessed*1.0d/(1024*1024*1024)
      val slotMinutes = stats.getTotalSlotMs*1.0d/(1000*60)
      val executionSeconds = queryDuration/1000d
      val queuedSeconds = waitDuration/1000d

      val sb = new StringBuilder(4096)
      sb.append(
        s"""
           |$statementType summary
           |$stageCount stages
           |$stepCount steps
           |$subStepCount sub-steps
           |${stageSummary.mkString("\n")}
           |
           |Utilization:
           |${f(gbProcessed)} GB processed
           |${shuffleBytesSpilled/(1024L*1024L*1024L)} GB spilled to disk during shuffle
           |${f(slotMinutes)} slot minutes consumed
           |${f(slotUtilizationRate)} slots utilized on average over the duration of the query
           |${f(slotMsToTotalBytesRatio)} ratio of slot ms to bytes processed
           |${f(shuffleBytesToTotalBytesRatio)} ratio of bytes shuffled to bytes processed
           |${f(shuffleSpillToShuffleBytesRatio)} ratio of bytes spilled to shuffle bytes (lower is better)
           |${f(shuffleSpillToTotalBytesRatio)} ratio of bytes spilled to bytes processed (lower is better)
           |
           |Timing:
           |${f(executionSeconds)} seconds execution time
           |${f(queuedSeconds)} seconds waiting in queue
           |
           |""".stripMargin)

      if (isMerge) {
        sb.append("Merge results:\n")
        for {n <- mergeIntoRows; t <- mergeIntoTable} yield {
          sb.append(s"$n rows read from ${BQ.tableSpec(t)} (dest)\n")
        }
        for {n <- mergeFromRows; t <- mergeFromTable} yield {
          sb.append(s"$n rows read from ${BQ.tableSpec(t)} (src)\n")
        }
        for {
          a <- mergeInsertedRows; b <- mergeUpdatedRows; c <- mergeAffectedRows; d <- mergeOutputRows
        } yield {
          sb.append(
            s"""$d rows output
               |$c rows affected
               |${d - c} rows unmodified
               |
               |$a rows inserted
               |$b rows updated
               |""".stripMargin)
        }
      } else if (isSelect) {
        sb.append("Select results:\n")
        val sources = selectFromTables.map(BQ.tableSpec).mkString("\n     ,")
        val dest = BQ.tableSpec(selectIntoTable)
        for {s <- selectInputRows} yield {
          for ((t,n) <- s){
            sb.append(s"$n rows from $t\n")
          }
        }
        if (sources.nonEmpty)
          sb.append(s"Input: $sources\n")
        if (dest.nonEmpty)
          sb.append(s"Destination: $dest\n")
        for {n <- selectOutputRows} yield {
          sb.append(s"Output rows: $n\n")
        }
      }

      CloudLogging.getLogger("StatsUtil")
        .infoJson(Seq(
          ("msgType", "queryStats"),
          ("query", conf.getQuery),
          ("statementType", statementType),
          ("gbProcessed", gbProcessed),
          ("slotMinutes", slotMinutes),
          ("slotUtilizationRate", slotUtilizationRate),
          ("slotMsToTotalBytesRatio", slotMsToTotalBytesRatio),
          ("shuffleBytesPerTotalBytes", shuffleBytesToTotalBytesRatio),
          ("shuffleSpillToShuffleBytes", shuffleSpillToShuffleBytesRatio),
          ("shuffleSpillToTotalBytes", shuffleSpillToTotalBytesRatio),
          ("shuffleSpillGB", shuffleBytesSpilled),
          ("executionSeconds", executionSeconds),
          ("queuedSeconds", queuedSeconds),
          ("stageCount", stageCount),
          ("stepCount", stepCount),
          ("subStepCount", subStepCount),
          ("stages", stageSummary.mkString(";")),
          ("jobId", BQ.toStr(job.getJobId),
          ("project", job.getJobId.getProject),
          ("location", job.getJobId.getLocation),
          ("destination", BQ.tableSpec(Option(conf.getDestinationTable))),
        ), null)

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
    row.put("job_id", BQ.toStr(jobId))
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
    bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(BQ.toStr(jobId), row.build).build()) match {
      case x if x.hasErrors =>
        val errors = x.getInsertErrors.asScala.values.flatMap(_.asScala).mkString("\n")
        logger.error(s"failed to insert stats for Job ID ${BQ.toStr(jobId)}\n$errors")
      case _ =>
        logger.debug(s"inserted job stats for Job ID ${BQ.toStr(jobId)}")
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
