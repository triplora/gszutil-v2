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

package com.google.cloud.bqsh.cmd

import com.google.cloud.bigquery._
import com.google.cloud.bqsh._
import com.ibm.jzos.ZFileProvider

object Mk extends Command[MkConfig]{
  override val name: String = "bq mk"
  override val parser: ArgParser[MkConfig] = MkOptionParser

  def run(cfg: MkConfig, zos: ZFileProvider): Result = {
    val creds = zos.getCredentialProvider().getCredentials
    val bq = BQ.defaultClient(cfg.projectId, cfg.location, creds)
    val tableId = BQ.resolveTableSpec(cfg.tablespec, cfg.projectId, cfg.datasetId)

    if (cfg.externalTableDefinition.nonEmpty){
      createExternalTable(bq, tableId, cfg.externalTableUri.map(_.toString), cfg.expiration)
    } else if (cfg.table) {
      createTable(bq, cfg, tableId)
    } else if (cfg.view) {
      val query = zos.readDDString("QUERY", " ")
      createView(bq, cfg, tableId, query)
    } else {
      throw new NotImplementedError(s"unsupported operation $cfg")
    }
    Result()
  }

  def createView(bq: BigQuery,
                 cfg: MkConfig,
                 tableId: TableId,
                 query: String): Table = {

    val tableDefinition = ViewDefinition.newBuilder(query)
      .build()

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition)
      .setDescription(cfg.description)

    if (cfg.expiration > 0)
      tableInfo.setExpirationTime(System.currentTimeMillis() + cfg.expiration)

    bq.create(tableInfo.build())
  }

  def createTable(bq: BigQuery,
                  cfg: MkConfig,
                  tableId: TableId): Table = {
    val tableDefinition = StandardTableDefinition
      .newBuilder()
      .setSchema(BQ.parseSchema(cfg.schema))
      .setLocation(cfg.location)
      .setType(TableDefinition.Type.TABLE)

    if (cfg.timePartitioningField.nonEmpty) {
      val b = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
        .setRequirePartitionFilter(cfg.requirePartitionFilter)
        .setField(cfg.timePartitioningField)

      if (cfg.timePartitioningExpiration > 0)
        b.setExpirationMs(cfg.timePartitioningExpiration)

      tableDefinition.setTimePartitioning(b.build())
    }

    if (cfg.clusteringFields.nonEmpty){
      import scala.collection.JavaConverters.seqAsJavaListConverter
      val b = Clustering.newBuilder().setFields(cfg.clusteringFields.asJava)
      tableDefinition.setClustering(b.build())
    }

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition.build)

    if (cfg.expiration > 0)
      tableInfo.setExpirationTime(System.currentTimeMillis() + cfg.expiration)

    bq.create(tableInfo.build())
  }

  def createExternalTable(bq: BigQuery,
                          tableId: TableId,
                          sources: Seq[String],
                          lifetimeMillis: Long): Table = {
    import scala.collection.JavaConverters.seqAsJavaListConverter

    val expirationTime = System.currentTimeMillis() + lifetimeMillis

    val tableDefinition = ExternalTableDefinition
      .newBuilder(sources.asJava, null, FormatOptions.orc())
      .build()

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition)
      .setExpirationTime(expirationTime)
      .build()

    bq.create(tableInfo)
  }
}
