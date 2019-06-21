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

package com.google.cloud.bqz

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.RetryOption
import com.google.cloud.bigquery._
import com.google.cloud.gszutil.CCATransportFactory
import com.google.cloud.http.HttpTransportOptions
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import org.threeten.bp.Duration

object BQ {
  def defaultClient(project: String, location: String, credentials: Credentials): BigQuery = {
    BigQueryOptions.newBuilder()
      .setLocation(location)
      .setProjectId(project)
      .setCredentials(credentials)
      .setTransportOptions(HttpTransportOptions.newBuilder()
        .setHttpTransportFactory(new CCATransportFactory)
        .build())
      .setRetrySettings(RetrySettings.newBuilder()
        .setMaxAttempts(8)
        .setTotalTimeout(Duration.ofMinutes(30))
        .setInitialRetryDelay(Duration.ofSeconds(8))
        .setMaxRetryDelay(Duration.ofSeconds(32))
        .setRetryDelayMultiplier(2.0d)
        .build())
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "gszutil-0.1"))
      .build()
      .getService
  }

  def runJob(bq: BigQuery, cfg: QueryJobConfiguration, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(bq)
    Preconditions.checkNotNull(cfg)
    Preconditions.checkNotNull(jobId)
    try {
      val job = bq.create(JobInfo.of(jobId, cfg))
      await(job, jobId, timeoutSeconds)
    } catch {
      case e: BigQueryException =>
        if (e.getMessage.startsWith("Already Exists")) {
          await(bq, jobId, timeoutSeconds)
        } else {
          throw new RuntimeException(e)
        }
    }
  }

  def await(bq: BigQuery, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(jobId)
    val job = bq.getJob(jobId)
    await(job, jobId, timeoutSeconds)
  }

  def await(job: Job, jobId: JobId, timeoutSeconds: Long): Job = {
    Preconditions.checkNotNull(jobId)
    if (job == null) {
      throw new RuntimeException(s"Job ${jobId.getJob} doesn't exist")
    } else {
      job.waitFor(RetryOption.totalTimeout(Duration.ofSeconds(timeoutSeconds)))
    }
  }

  def resolveTableSpec(tableSpec: String, defaultProject: String, defaultDataset: String): TableId = {
    val i = tableSpec.indexOf(':')
    val j = tableSpec.indexOf('.')

    val project = if (i > 0) tableSpec.substring(0, i) else defaultProject
    val dataset = if (j > 0) tableSpec.substring(i+1, j) else defaultDataset
    val table = if (j > 0) tableSpec.substring(j+1, tableSpec.length) else tableSpec
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
    schemaUpdateOptionsBuilder.build()
  }

  def parseField(s: String): Field = {
    val Array(field,dataType) = s.split(':')
    val fieldList = FieldList.of()
    val typeName = StandardSQLTypeName.valueOf(dataType)
    Field.newBuilder(field,typeName,fieldList).build()
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
