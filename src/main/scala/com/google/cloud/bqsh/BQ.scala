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

package com.google.cloud.bqsh

import java.time.LocalDateTime

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableReference
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, DatasetId, Field, FieldList, InsertAllRequest, InsertAllResponse, Job, JobId, JobInfo, JobStatus, QueryJobConfiguration, Schema, StandardSQLTypeName, StandardTableDefinition, TableDefinition, TableId}
import com.google.cloud.imf.gzos.{MVS, Util}
import com.google.cloud.imf.util.StatsUtil.EnhancedJob
import com.google.cloud.imf.util.{CCATransportFactory, Logging, StaticMap, StatsUtil}
import com.google.common.collect.{ImmutableList, ImmutableMap}
import com.google.api.services.{bigquery => bqapi}
import com.google.api.services.bigquery.{model => bqmdl}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.{Failure, Success, Try}

object BQ extends Logging {
  def genJobId(projectId: String, location: String, zos: MVS, jobType: String): JobId = {
    val t = LocalDateTime.now
    val job = zos.getInfo
    val jobId = Seq(
      job.getJobname,job.getStepName,job.getJobid,jobType,s"${t.getHour}${t.getMinute}${t.getSecond}"
    ).mkString("_")
    JobId.newBuilder.setProject(projectId).setLocation(location).setJob(jobId).build
  }

  /** Converts TableId to String */
  def tableSpec(t: TableId): String = s"${t.getProject}.${t.getDataset}.${t.getTable}"

  /** Converts Option[TableId] to String */
  def tableSpec(t: Option[TableId]): String = t.map(tableSpec).getOrElse("")
  def tableSpec(t: bqmdl.TableReference): String = s"${t.getProjectId}.${t.getDatasetId}.${t.getTableId}"

  def tablesEqual(t: TableId, t1: TableId): Boolean = {
    t1.getProject == t.getProject &&
    t1.getDataset == t.getDataset &&
    t1.getTable == t.getTable
  }

  def tablesEqual(t0: TableId, t1: Option[TableId]): Boolean = {
    t1.exists{t => tablesEqual(t0, t)}
  }

  /** Counts rows in a table
    * @param bq BigQuery
    * @param table TableId
    * @return row count
    */
  def rowCount(bq: BigQuery, table: TableId): Long = {
    val tbl = bq.getTable(table)
    val n = tbl.getNumRows.longValueExact
    val nBytes = tbl.getNumBytes.longValue
    val ts = StatsUtil.epochMillis2Timestamp(tbl.getLastModifiedTime)
    logger.info(s"${tableSpec(table)} contains $n rows $nBytes bytes as of $ts")
    n
  }

  /** Submits a query with Dry Run enabled
    *
    * @param bq BigQuery client instance
    * @param cfg QueryJobConfiguration
    * @param jobId QueryJobConfiguration
    * @param timeoutSeconds maximum wait time
    * @return
    */
  def queryDryRun(bq: BigQuery,
                  cfg: QueryJobConfiguration,
                  jobId: JobId,
                  timeoutSeconds: Long): EnhancedJob = {
    val jobId1 = jobId.toBuilder.setJob(jobId.getJob+"_dryrun")
    bq.create(JobInfo.of(jobId1.build, cfg.toBuilder.setDryRun(true).build))
    val queryJob = waitForJob(bq, jobId, timeoutMillis = timeoutSeconds * 1000L)
    new EnhancedJob(queryJob)
  }

  /** Submits a BigQuery job and waits for completion
    * returns a completed job or throws exception
    *
    * @param bq BigQuery client instance
    * @param cfg QueryJobConfiguration
    * @param jobId JobId
    * @param timeoutSeconds maximum time to wait for status change
    * @param sync if set to false, return without waiting for status change
    * @return Job
    */
  def runJob(bq: BigQuery,
             cfg: QueryJobConfiguration,
             jobId: JobId,
             timeoutSeconds: Long,
             sync: Boolean): Job = {
    try {
      val job = bq.create(JobInfo.of(jobId, cfg))
      if (sync) {
        logger.info(s"Waiting for Job jobid=${BQ.toStr(jobId)}")
        waitForJob(bq, jobId, timeoutMillis = timeoutSeconds * 1000L)
      } else {
        logger.info(s"Returning without waiting for job to complete because sync=false jobid=${BQ.toStr(jobId)}")
        job
      }
    } catch {
      case e: BigQueryException =>
        if (e.getReason == "duplicate" && e.getMessage.startsWith("Already Exists: Job")) {
          logger.warn(s"Job already exists, waiting for completion.\njobid=${BQ.toStr(jobId)}")
          waitForJob(bq, jobId, timeoutMillis = timeoutSeconds * 1000L)
        } else {
          throw new RuntimeException(e)
        }
    }
  }

  def toStr(j: JobId): String =
    s"${j.getProject}:${j.getLocation}.${j.getJob}"

  @tailrec
  def waitForJob(bq: BigQuery,
                 jobId: JobId,
                 waitMillis: Long = 2000,
                 maxWaitMillis: Long = 60000,
                 timeoutMillis: Long = 300000,
                 retries: Int = 120): Job = {
    if (timeoutMillis <= 0){
      throw new RuntimeException(s"Timed out waiting for ${toStr(jobId)}")
    } else if (retries <= 0){
      throw new RuntimeException(s"Timed out waiting for ${toStr(jobId)} - retry limit reached")
    }
    Try{Option(bq.getJob(jobId))} match {
      case Success(Some(job)) if isDone(job) =>
        logger.info(s"${toStr(jobId)} Status = DONE")
        job
      case Failure(e) =>
        logger.error(s"BigQuery Job failed jobid=${toStr(jobId)}\n" +
          s"message=${e.getMessage}", e)
        throw e
      case Success(None) =>
        throw new RuntimeException(s"BigQuery Job not found jobid=${toStr(jobId)}")
      case Success(_) =>
        Util.sleepOrYield(waitMillis)
        waitForJob(bq, jobId, math.min(waitMillis * 2, maxWaitMillis),
          maxWaitMillis, timeoutMillis - waitMillis, retries - 1)
    }
  }

  def isDone(job: Job): Boolean =
    job.getStatus != null && job.getStatus.getState == JobStatus.State.DONE

  def hasError(job: Job): Boolean = {
    Option(job.getStatus)
      .flatMap(x => Option(x.getError))
      .isDefined ||
    hasExecutionErrors(job)
  }

  def hasExecutionErrors(job: Job): Boolean = {
    Option(job.getStatus)
      .flatMap(x => Option(x.getExecutionErrors))
      .map(_.size())
      .exists(_ > 0)
  }

  case class BQError(message: Option[String],
                     reason: Option[String],
                     location: Option[String]) {
    override def toString: String = {
      s"BigQueryError:\nreason: ${reason.getOrElse("no reason given")}\nmessage: ${message.getOrElse("no message given")}"
    }
  }

  case class BQStatus(state: JobStatus.State,
                      error: Option[BQError],
                      executionErrors: Seq[BQError]) {
    def hasError: Boolean = error.isDefined || executionErrors.nonEmpty
    def isDone: Boolean = state == JobStatus.State.DONE
  }

  def getExecutionErrors(status: JobStatus): Seq[BQError] = {
    Option(status)
      .flatMap(x => Option(x.getExecutionErrors))
      .map(_.asScala.flatMap(toBQError).toArray.toSeq)
      .getOrElse(Seq.empty)
  }

  def toBQError(error: BigQueryError): Option[BQError] = {
    if (error == null) None
    else Option(BQError(
      message = Option(error.getMessage),
      reason = Option(error.getReason),
      location = Option(error.getLocation)
    ))
  }

  def getStatus(job: Job): Option[BQStatus] = {
    val status = job.getStatus
    if (status == null) None
    else {
      Option(BQStatus(
        state = status.getState,
        error = toBQError(status.getError),
        executionErrors = getExecutionErrors(status)))
    }
  }

  def throwOnError(job: EnhancedJob, status: BQStatus): Unit = {
    val sb = new StringBuilder
    if (status.hasError) {
      status.error.foreach { err =>
        sb.append(err.toString)
      }
      if (status.executionErrors.nonEmpty) {
        sb.append("Execution Errors:\n")
      }
      status.executionErrors.foreach { err =>
        sb.append(err.toString)
      }
      val errMsgs = sb.result

      sb.clear()
      sb.append(s"${job.id} has errors\n")
      sb.append("Query:\n")
      sb.append(job.query)
      sb.append("\n")
      sb.append("Errors:\n")
      sb.append(errMsgs)

      logger.error(sb.result)
      throw new RuntimeException(errMsgs)
    }
  }

  /**
    *
    * @param datasetSpec [PROJECT]:[DATASET] may override project
    * @param defaultProject BigQuery billing project
    * @return
    */
  def resolveDataset(datasetSpec: String, defaultProject: String): DatasetId = {
    val i =
      if (datasetSpec.contains('.'))
        datasetSpec.indexOf('.')
      else datasetSpec.indexOf(':')
    val project = if (i > 0) datasetSpec.substring(0, i) else defaultProject
    val dataset = datasetSpec.substring(i+1)
    DatasetId.of(project, dataset)
  }

  /** Read TableId from String, allowing for either : or . as delimiters
    *
    * @param tableSpec project:dataset.table or project.dataset.table
    * @param defaultProject project to use if not provided by tablespec or dataset
    * @param defaultDataset project:dataset or project.dataset may override project
    * @return
    */
  def resolveTableSpec(tableSpec: String,
                       defaultProject: String,
                       defaultDataset: String): TableId = {
    val i =
      if (tableSpec.count(_ == '.') == 2)
        tableSpec.indexOf('.')
      else tableSpec.indexOf(':')
    val j = tableSpec.indexOf('.',i+1)

    val default =
      if (defaultProject.nonEmpty && defaultDataset.nonEmpty) {
        val datasetId = resolveDataset(defaultDataset, defaultProject)
        Option(datasetId)
      } else {
        logger.warn("default dataset not set")
        None
      }

    val project =
      if (i > 0) tableSpec.substring(0, i)
      else default.map(_.getProject).getOrElse(defaultProject)

    val dataset =
      if (j > 0) tableSpec.substring(i+1, j)
      else default.map(_.getDataset).getOrElse(defaultDataset)

    val table =
      if (j > 0) tableSpec.substring(j+1, tableSpec.length)
      else tableSpec
    TableId.of(project, dataset, table)
  }

  def parseSchemaUpdateOption(schemaUpdateOption: Seq[String]): java.util.List[JobInfo.SchemaUpdateOption] = {
    val schemaUpdateOptionsBuilder = ImmutableList.builder[JobInfo.SchemaUpdateOption]()
    if (schemaUpdateOption.contains(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION.name())) {
      schemaUpdateOptionsBuilder
        .add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION)
    }
    if (schemaUpdateOption.contains(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION.name())) {
      schemaUpdateOptionsBuilder
        .add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)
    }
    schemaUpdateOptionsBuilder.build
  }

  def parseField(s: String): Field = {
    val Array(field,dataType) = s.split(':')
    val fieldList = FieldList.of()
    val typeName = StandardSQLTypeName.valueOf(dataType)
    Field.newBuilder(field,typeName,fieldList).build
  }

  def parseSchema(s: Seq[String]): Schema = {
    Schema.of(s.map(parseField):_*)
  }

  def isValidTableName(s: String): Boolean = {
    s.matches("[a-zA-Z0-9_]{1,1024}")
  }

  def isValidBigQueryName(s: String): Boolean =
    s.matches("[a-zA-Z0-9_]{1,1024}")

  case class BQSchema(fields: Seq[BQField] = Nil) {
    lazy val fieldNames: Set[String] = fields.map(_.name).toSet
    def contains(name: String): Boolean = fieldNames.contains(name)
  }

  case class BQField(name: String, typ: StandardSQLTypeName = StandardSQLTypeName.STRING, mode: Field
  .Mode = Field.Mode
    .NULLABLE)

  /** Builder meant to be used with streaming insert
    * Won't insert columns that don't exist in the schema
    */
  case class SchemaRowBuilder(schema: Option[BQSchema] = None) {
    private var row = StaticMap.builder
    def put(name: String, value: Any): SchemaRowBuilder = {
      if (schema.isDefined && schema.get.contains(name) && value != null) {
        value match {
          case None =>
          case s: String if s.isEmpty =>
          case _ =>
            row.put(name, value)
        }
      }
      this
    }
    def build: java.util.Map[String,Any] = row.build()
    def clear(): Unit = row = StaticMap.builder
    def insert(bq: BigQuery, tableId: TableId, id: String): InsertAllResponse =
      bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(id, row.build).build)
  }

  def getFields(x: TableDefinition): BQSchema = {
    val buf = ListBuffer.empty[BQField]
    x.getSchema.getFields.forEach{f =>
      if (f.getType == null)
        logger.warn(s"field type not set for ${f.getName}")
      else
        buf.append(BQField(f.getName, f.getType.getStandardType, f.getMode))
    }

    if (x.getSchema == null)
      logger.warn(s"schema not found for standard table")
    BQSchema(buf.result)
  }

  def checkTableDef(bq: BigQuery, tableId: TableId, expectedSchema: BQSchema): Option[BQSchema] = {
    val ts = BQ.tableSpec(tableId)
    Option(bq.getTable(tableId)) match {
      case Some(table) =>
        table.getDefinition[TableDefinition] match {
          case x: StandardTableDefinition =>
            val schema = getFields(x)
            if (schema != expectedSchema) {
              logger.warn(s"Schema mismatch\nExpected:\n$expectedSchema\n\nGot:\n$schema")
              Option(schema)
            } else Option(schema)
          case x =>
            val typ = x.getClass.getSimpleName.stripSuffix("$").stripSuffix("Definition")
            logger.warn(s"stats table is a $typ but should be a standard table. tablespec=$ts")
            None
        }
      case None =>
        logger.warn(s"stats table not found. tablespec=$ts")
        None
    }
  }

  def apiClient(credentials: Credentials): com.google.api.services.bigquery.Bigquery =
    new com.google.api.services.bigquery.Bigquery(CCATransportFactory.getTransportInstance,
      JacksonFactory.getDefaultInstance, new HttpCredentialsAdapter(credentials))

  def apiGetJob(bq: com.google.api.services.bigquery.Bigquery, jobId: JobId): com.google
  .api.services.bigquery.model.Job = {
    val req = bq.jobs().get(jobId.getProject, jobId.getJob)
    req.setLocation(jobId.getLocation)
    req.execute()
  }

  def getJobJson(bq: bqapi.Bigquery, jobId: JobId): Option[String] = {
    try {
      val res = apiGetJob(bq,jobId)
      if (res != null) {
        Option(JacksonFactory.getDefaultInstance.toString(res))
      }
      else None
    } catch {
      case _: Throwable => None
    }
  }

  trait HasStats {
    def addToRow(row: SchemaRowBuilder): Unit
  }

  class EnhancedJobStatistics(stats: bqmdl.JobStatistics) extends HasStats {
    def queryDurationMs: Long = stats.getEndTime - stats.getStartTime
    def waitDurationMs: Long = stats.getStartTime - stats.getCreationTime
    override def addToRow(row: SchemaRowBuilder): Unit = {
    }
  }

  class EnhancedExplainQueryStep(step: bqmdl.ExplainQueryStep) {
    def readsFrom(t: TableId): Boolean = readsFrom(BQ.tableSpec(t))
    def readsFrom(tableSpec: String): Boolean = {
      if (step.getKind == null || step.getSubsteps == null) false
      else {
        step.getKind == "READ" &&
          step.getSubsteps.asScala.exists{subStep =>
            subStep.startsWith("FROM ") &&
              subStep.endsWith(tableSpec)
          }
      }
    }
  }

  class EnhancedExplainQueryStage(stage: bqmdl.ExplainQueryStage) {
    def readsFrom(t: TableId): Boolean = readsFrom(BQ.tableSpec(t))
    def readsFrom(t: TableReference): Boolean = readsFrom(BQ.tableSpec(t))
    def readsFrom(tableSpec: String): Boolean =
      stage.getSteps.asScala.exists(_.readsFrom(tableSpec))
  }

  import scala.language.implicitConversions

  implicit def enhanceExplainQueryStep(x: bqmdl.ExplainQueryStep): EnhancedExplainQueryStep =
    new EnhancedExplainQueryStep(x)

  implicit def enhanceExplainQueryStage(x: bqmdl.ExplainQueryStage): EnhancedExplainQueryStage =
    new EnhancedExplainQueryStage(x)

  class JobStats(j: bqmdl.Job) extends HasStats {
    override def addToRow(row: SchemaRowBuilder): Unit = {
      row
        .put("bq_project", j.getJobReference.getProjectId)
        .put("bq_location", j.getJobReference.getLocation)
        .put("bq_job_id", j.getJobReference.getJobId)
        .put("job_json", JacksonFactory.getDefaultInstance.toString(j))

      if (j.getConfiguration.getQuery != null)
        new QueryStats(j).addToRow(row)
      else if (j.getConfiguration.getLoad != null)
        new LoadStats(j).addToRow(row)
    }
  }

  class LoadStats(j: bqmdl.Job) extends HasStats {
    override def addToRow(row: SchemaRowBuilder): Unit = {
      row
        .put("source", j.getConfiguration.getLoad.getSourceUris.asScala.mkString(","))
        .put("destination", tableSpec(j.getConfiguration.getLoad.getDestinationTable))
        .put("rows_loaded", j.getStatistics.getLoad.getOutputRows)
    }
  }

  class QueryStats(j: bqmdl.Job) extends HasStats {
    def s1: bqmdl.JobStatistics = j.getStatistics
    def s2: bqmdl.JobStatistics2 = j.getStatistics.getQuery

    def queryDurationMs: Long = s1.getEndTime - s1.getStartTime
    def waitDurationMs: Long = s1.getStartTime - s1.getCreationTime

    def slotMsToTotalBytesRatio: Double =
      if (s2.getTotalBytesProcessed > 0)
        (s2.getTotalSlotMs * 1.0d) / s2.getTotalBytesProcessed
      else 0

    def slotUtilizationRate: Double =
      if (queryDurationMs > 0)
        (s2.getTotalSlotMs * 1.0d) / queryDurationMs
      else 0

    def stageSummary: Seq[String] = {
      s2.getQueryPlan.asScala.map{stage =>
        val tbls = stage.getSteps.asScala.flatMap{ step =>
          val last = step.getSubsteps.asScala.last
          val tbl = last.stripPrefix("FROM ")
          if (step.getKind == "READ" && last.startsWith("FROM ") && !tbl.startsWith("__")) {
            Option(tbl)
          } else None
        }
        val stepCount = stage.getSteps.size
        val subStepCount = stage.getSteps.asScala.foldLeft(0){(a,b) =>
          a + b.getSubsteps.size
        }
        val inputs = if (tbls.nonEmpty) tbls.mkString(" inputs:",",","") else ""
        s"${stage.getName} in:${stage.getRecordsRead} out:${stage.getRecordsWritten} " +
          s"steps:$stepCount subSteps:$subStepCount$inputs"
      }.toSeq
    }

    def stageCount: Int = s2.getQueryPlan.size

    def stepCount: Int = s2.getQueryPlan.asScala.foldLeft(0){ (a, b) =>
      a + b.getSteps.size
    }

    def subStepCount: Int = s2.getQueryPlan.asScala.foldLeft(0){ (a, b) =>
      a + b.getSteps.asScala.foldLeft(0){(c,d) =>
        c + d.getSubsteps.size()
      }
    }

    def bytesProcessed: Long = s2.getTotalBytesProcessed

    def shuffleBytes: Long =
      s2.getQueryPlan.asScala.foldLeft(0L){ (a, b) => a + b.getShuffleOutputBytes}

    def shuffleBytesSpilled: Long =
      s2.getQueryPlan.asScala.foldLeft(0L){ (a, b) => a + b.getShuffleOutputBytesSpilled}

    def shuffleBytesToTotalBytesRatio: Double =
      if (s2.getTotalBytesProcessed > 0)
        shuffleBytes / s2.getTotalBytesProcessed
      else 0

    def shuffleSpillToShuffleBytesRatio: Double =
      if (shuffleBytes > 0)
        shuffleBytesSpilled / shuffleBytes
      else 0

    def shuffleSpillToTotalBytesRatio: Double =
      if (s2.getTotalBytesProcessed > 0)
        shuffleBytesSpilled / s2.getTotalBytesProcessed
      else 0

    override def addToRow(row: SchemaRowBuilder): Unit = {
      row
        .put("rows_affected", s2.getNumDmlAffectedRows)

      if (s2.getStatementType == "MERGE")
        new MergeStats(j).addToRow(row)
      else if (s2.getStatementType == "MERGE")
        new SelectStats(j).addToRow(row)

      row
        .put("statement_type", s2.getStatementType)
        .put("query", j.getConfiguration.getQuery.getQuery)
        .put("execution_hours", ms2Hr(queryDurationMs))
        .put("queued_hours", ms2Hr(waitDurationMs))
        .put("gb_processed", b2GB(s2.getTotalBytesProcessed))
        .put("slot_hours", ms2Hr(j.getStatistics.getTotalSlotMs))
        .put("slot_utilization_rate", slotUtilizationRate)
        .put("slot_ms_to_total_bytes_ratio", slotMsToTotalBytesRatio)
        .put("shuffle_bytes_to_total_bytes_ratio", shuffleBytesToTotalBytesRatio)
        .put("shuffle_spill_bytes_to_shuffle_bytes_ratio", shuffleSpillToShuffleBytesRatio)
        .put("shuffle_spill_bytes_to_total_bytes_ratio", shuffleSpillToTotalBytesRatio)
        .put("shuffle_spill_gb", b2GB(shuffleBytesSpilled))
        .put("bq_stage_count", stageCount)
        .put("bq_step_count", stepCount)
        .put("bq_sub_step_count", subStepCount)
        .put("bq_stage_summary", stageSummary.mkString("\n"))
    }
  }

  def b2GB(b: Long): Double = b.toDouble / (1024*1024*1024)
  def ms2Hr(ms: Long): Double = ms.toDouble / (1000*60*60)

  class MergeStats(j: bqmdl.Job) extends HasStats {
    require(j.getStatus != null && j.getStatus.getState == "DONE", "job is not done")
    require(j.getStatistics != null && j.getStatistics.getQuery != null && j.getStatistics
      .getQuery.getStatementType != null && j.getStatistics
      .getQuery.getStatementType == "MERGE", "job is not a merge job")

    def stats: bqmdl.JobStatistics2 = j.getStatistics.getQuery
    def plan = stats.getQueryPlan.asScala

    def destTable: TableReference = j.getConfiguration.getQuery.getDestinationTable
    def sourceTable: TableReference = stats.getReferencedTables.asScala.find{ t =>
      t.getTableId != destTable.getTableId ||
        t.getDatasetId != destTable.getDatasetId ||
        t.getProjectId != destTable.getProjectId
    }.get

    def rowsRead: Long =
      plan.find{x =>
        x.readsFrom(destTable) &&
        x.getSteps.asScala.count(_.getKind == "READ") == 1
      }.get.getRecordsRead

    def rowsWritten: Long =
      plan.flatMap{x =>
        if (x.getName.endsWith(": Output") && x.getRecordsWritten != null)
          Option(x.getRecordsWritten.toLong)
        else None
      }.lastOption.get

    def rowsInserted: Long = rowsWritten - rowsRead
    def rowsAffected: Long = stats.getNumDmlAffectedRows
    def rowsUpdated: Long = rowsAffected - rowsInserted
    def rowsUnmodified: Long = rowsWritten - rowsAffected

    override def addToRow(row: SchemaRowBuilder): Unit = {
      row
        .put("rows_read", rowsRead)
        .put("rows_written", rowsWritten)
        .put("rows_inserted", rowsInserted)
        .put("rows_updated", rowsUpdated)
        .put("rows_unmodified", rowsUnmodified)
    }
  }

  class SelectStats(j: bqmdl.Job) extends HasStats {
    def plan = j.getStatistics.getQuery.getQueryPlan.asScala

    def destTable: TableReference =
      j.getConfiguration.getQuery.getDestinationTable

    def sourceTables: Seq[TableReference] =
      j.getStatistics.getQuery.getReferencedTables.asScala.toSeq

    def inputs: Seq[(String,Long)] = {
      plan.flatMap{s =>
        s.getSteps.asScala.flatMap{step =>
          val isRead = step.getKind == "READ"
          val lastSubStep = step.getSubsteps.asScala.last
          val isFrom = lastSubStep.startsWith("FROM")
          val table = lastSubStep.stripPrefix("FROM ")
          val fromTable = !table.startsWith("__")
          if (isRead && isFrom && fromTable){
            Option((table, s.getRecordsRead.toLong))
          } else None
        }
      }.toList
    }

    def rowsRead: Long = inputs.foldLeft(0L)(_ + _._2)

    def rowsWritten: Long =
      plan.foldLeft(0L){(a,b) =>
        if (b.getName.endsWith(": Output") && b.getRecordsWritten != null)
          b.getRecordsWritten
        else a
      }

    override def addToRow(row: SchemaRowBuilder): Unit = {
      row
        .put("rowsRead", rowsRead)
        .put("rowsWritten", rowsWritten)
    }
  }
}
