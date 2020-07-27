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

import com.google.cloud.imf.gzos.MVSStorage.{DSN, MVSDataset, MVSPDSMember}
import com.google.cloud.imf.util.StaticMap

object QueryConfig {
  def create(sql: String, datasetId: String, location: String, projectId: String,
             statsTable: String, replace: Boolean, destinationTable: String): QueryConfig = {
    QueryConfig(sql, datasetId = datasetId, location = location, projectId = projectId,
      statsTable = statsTable, replace = replace, destinationTable = destinationTable)
  }

  /** QueryJobConfiguration with DML may not set WriteDisposition
    * therefore replace option is not allowed for DML queries
    */
  def dml(dml: String, datasetId: String, location: String, projectId: String,
             statsTable: String, destinationTable: String): QueryConfig = {
    QueryConfig(dml, datasetId = datasetId, location = location, projectId = projectId,
      statsTable = statsTable, destinationTable = destinationTable)
  }
}

case class QueryConfig(
  // Custom Options
  sql: String = "",
  queryDSN: String = "",
  timeoutMinutes: Int = 60,
  parametersFromFile: Seq[String] = Seq.empty,
  createIfNeeded: Boolean = false,
  allowMultipleQueries: Boolean = false,

  // Standard Options
  allowLargeResults: Boolean = false,
  appendTable: Boolean = false,
  batch: Boolean = false,
  clusteringFields: Seq[String] = Seq.empty,
  destinationKmsKey: String = "",
  destinationSchema: String = "",
  destinationTable: String = "",
  dryRun: Boolean = false,
  externalTableDefinition: String = "",
  label: String = "",
  maximumBytesBilled: Long = -1,
  parameters: Seq[String] = Seq.empty,
  replace: Boolean = false,
  requireCache: Boolean = false,
  requirePartitionFilter: Boolean = true,
  schemaUpdateOption: Seq[String] = Seq.empty,
  timePartitioningExpiration: Long = -1,
  timePartitioningField: String = "",
  timePartitioningType: String = "",
  useCache: Boolean = true,
  useLegacySql: Boolean = false,

  // Global Options
  datasetId: String = "",
  debugMode: Boolean = false,
  jobId: String = "",
  jobProperties: Map[String,String] = Map.empty,
  location: String = "",
  projectId: String = "",
  sync: Boolean = true,

  statsTable: String = ""
) {
  def dsn: Option[DSN] = {
    val i = queryDSN.indexOf('(')
    val j = queryDSN.indexOf(')')
    if (i > 1 && j > i+1){
      Option(MVSPDSMember(queryDSN.substring(0,i),queryDSN.substring(i+1,j)))
    } else if (i > 0 && j > i+1) {
      Option(MVSDataset(queryDSN))
    } else None
  }

  def toMap: java.util.Map[String,Any] = {
    val m = StaticMap.builder
    m.put("type","QueryConfig")
    m.put("sql",sql)
    m.put("location",location)
    m.put("projectId",projectId)
    m.put("datasetId",datasetId)
    if (jobId.nonEmpty)
      m.put("jobId",jobId)
    if (statsTable.nonEmpty)
      m.put("statsTable",statsTable)
    m.build()
  }
}
