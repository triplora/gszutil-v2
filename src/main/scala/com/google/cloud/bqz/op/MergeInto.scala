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

package com.google.cloud.bqz.op

import com.google.cloud.bigquery._
import com.google.cloud.bqz.BQ
import com.google.cloud.gszutil.Util.{CredentialProvider, Logging}
import com.google.cloud.gszutil.{Config, Util}

object MergeInto extends Logging {
  def run(c: Config, cp: CredentialProvider): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, cp.getCredentials)
    val source = TableId.of(c.bqProject2, c.bqDataset2, c.bqTable2)
    val target = TableId.of(c.bqProject, c.bqDataset, c.bqTable)
    val mergeRequest = buildMergeRequest(bq, source, target, c.nativeKeyColumns)

    val query = genMerge(mergeRequest)
    logger.debug("Generated SQL:\n" + query)

    val cfg = QueryJobConfiguration.newBuilder(query)
      .setUseLegacySql(false)
      .setAllowLargeResults(true)
      .setUseQueryCache(false)
      .setPriority(QueryJobConfiguration.Priority.BATCH)
      .setDryRun(c.dryRun)
      .build()

    val jobId: JobId = JobId.of(s"gszutil_merge_into_${target.getDataset}_${target.getTable}_${System.currentTimeMillis()/1000}_${Util.randString(6)}")
    BQ.runJob(bq, cfg, jobId, 60*60)
  }

  case class MergeRequest(a: TableId, b: TableId, naturalKeyCols: Seq[String], valCols: Seq[String])

  def getSchema(t: Table): Schema = {
    if (t.getDefinition == null) {
      t.reload()
    }
    t.getDefinition[TableDefinition].getSchema
  }

  def buildMergeRequest(bq: BigQuery, source: TableId, target: TableId, naturalKeyCols: Seq[String]): MergeRequest = {
    val tblA = bq.getTable(target)
    val tblB = bq.getTable(source)
    val schemaA = getSchema(tblA)
    val schemaB = getSchema(tblB)
    val fields = getFields(schemaA)
    val naturalKeySet = naturalKeyCols.toSet
    validateMerge(fields.toMap, getFields(schemaB).toMap, naturalKeySet)
    val valCols = fields.map(_._1).filterNot(naturalKeySet.contains)
    MergeRequest(target, source, naturalKeyCols, valCols)
  }

  def validateMerge(fieldsA: Map[String,String], fieldsB: Map[String,String], naturalKeyCols: Set[String]): Boolean = {
    require(fieldsA == fieldsB, "schema mismatch")
    require(naturalKeyCols.forall(fieldsA.contains), "missing key column")
    require(fieldsA.keySet.diff(naturalKeyCols).nonEmpty, "no columns to merge")
    true
  }

  def getFields(f: Schema): Seq[(String,String)] = {
    import scala.collection.JavaConverters._
    f.getFields.iterator().asScala.toArray
      .map{x => (x.getName, x.getType.getStandardType.toString)}
  }

  def genMerge(request: MergeRequest): String = {
    import request._
    val naturalKeys = naturalKeyCols
      .map{colName => s"A.$colName\t=\tB.$colName"}
      .mkString("\t\t","\n\tAND\t", "")

    val values = valCols
      .map{colName => s"A.$colName\t=\tB.$colName"}
      .mkString("\t","\n\t,","")

    s"""MERGE INTO `${a.getProject}.${a.getDataset}.${a.getTable}` A
       |USING `${b.getProject}.${b.getDataset}.${b.getTable}` B
       |ON
       |$naturalKeys
       |WHEN MATCHED THEN
       |UPDATE SET
       |$values
       |WHEN NOT MATCHED THEN
       |INSERT ROW""".stripMargin
  }
}
