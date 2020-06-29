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

import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, BigQueryOptions, DatasetId, Field, FieldList, Job, JobId, JobInfo, JobStatus, QueryJobConfiguration, Schema, StandardSQLTypeName, TableId}
import com.google.cloud.imf.gzos.{MVS, Util}
import com.google.cloud.imf.util.Logging
import com.google.common.collect.ImmutableList

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.{Failure, Success, Try}

object BQ extends Logging {
  def genJobId(zos: MVS, jobType: String): JobId = {
    val t = System.currentTimeMillis
    val rand = Util.randString(5)
    JobId.of(s"${zos.jobId}_${jobType}_${t}_$rand")
  }

  def runJob(bq: BigQuery,
             cfg: QueryJobConfiguration,
             jobId: JobId,
             timeoutSeconds: Long,
             sync: Boolean): Job = {
    require(bq != null, "BigQuery must not be null")
    require(cfg != null, "QueryJobConfiguration must not be null")
    require(jobId != null, "JobId must not be null")
    try {
      val job = bq.create(JobInfo.of(jobId, cfg))
      if (sync) waitForJob(bq, jobId, timeoutMillis = timeoutSeconds * 1000L)
      else job
    } catch {
      case e: BigQueryException =>
        if (e.getReason == "duplicate" && e.getMessage.startsWith("Already Exists: Job")) {
          waitForJob(bq, jobId, timeoutMillis = timeoutSeconds * 1000L)
        } else {
          throw new RuntimeException(e)
        }
    }
  }

  @tailrec
  def waitForJob(bq: BigQuery,
                 jobId: JobId,
                 waitMillis: Long = 2000,
                 maxWaitMillis: Long = 30000,
                 timeoutMillis: Long = 300000,
                 retries: Int = 30): Job = {
    if (timeoutMillis <= 0){
      throw new RuntimeException(s"Timed out waiting for ${jobId.getJob}")
    } else if (retries <= 0){
      throw new RuntimeException(s"Retry limit reached waiting for ${jobId.getJob}")
    }
    logger.debug(s"waiting for ${jobId.getJob}")
    Try{Option(bq.getJob(jobId))} match {
      case Success(Some(job)) if isDone(job) =>
        logger.info(s"${jobId.getJob} Status = DONE")
        job
      case Failure(e) =>
        logger.error(e.getMessage, e)
        throw e
      case Success(_) =>
        Util.sleepOrYield(waitMillis)
        waitForJob(bq, jobId, math.min(waitMillis * 2, maxWaitMillis),
          maxWaitMillis, timeoutMillis - waitMillis, retries - 1)
    }
  }

  def isDone(job: Job): Boolean =
    Option(job.getStatus)
      .flatMap(x => Option(x.getState))
      .contains(JobStatus.State.DONE)

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

  def throwOnError(status: BQStatus): Unit = {
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
      throw new RuntimeException(sb.result())
    }
  }

  def throwOnError(job: Job): Unit = {
    getStatus(job) match {
      case Some(status) =>
        throwOnError(status)
      case _ =>
        throw new RuntimeException("missing job status")
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
    logger.debug(s"resolving tablespec $tableSpec")
    val i =
      if (tableSpec.count(_ == '.') == 2)
        tableSpec.indexOf('.')
      else tableSpec.indexOf(':')
    val j = tableSpec.indexOf('.',i+1)
    logger.debug(s"located tablespec separators: $i $j")

    val default =
      if (defaultProject.nonEmpty && defaultDataset.nonEmpty) {
        val datasetId = resolveDataset(defaultDataset, defaultProject)
        logger.debug(s"resolved default dataset ${datasetId.getProject}:${datasetId.getDataset}")
        Option(datasetId)
      } else {
        logger.debug("default dataset not set")
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
    logger.debug(s"resolved `$project:$dataset.$table`")
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
