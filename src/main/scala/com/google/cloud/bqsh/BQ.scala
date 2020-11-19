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

import java.io.{PrintWriter, StringWriter}
import java.time.LocalDateTime

import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, DatasetId, Field, FieldList, Job, JobId, JobInfo, JobStatus, QueryJobConfiguration, Schema, StandardSQLTypeName, TableId}
import com.google.cloud.imf.gzos.{MVS, Util}
import com.google.cloud.imf.util.{CloudLogging, Logging, StatsUtil}
import com.google.cloud.imf.util.StatsUtil.EnhancedJob
import com.google.common.collect.ImmutableList

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.{Failure, Random, Success, Try}

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
    CloudLogging.stdout(s"${tableSpec(table)} contains $n rows $nBytes bytes as of $ts")
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
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        val stackTrace = sw.toString
        logger.error(s"BigQuery Job failed jobid=${toStr(jobId)}\n" +
          s"${e.getMessage}\n$stackTrace", e)
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

      CloudLogging.stderr(sb.result)
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
}
