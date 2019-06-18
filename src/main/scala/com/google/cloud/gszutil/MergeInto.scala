package com.google.cloud.gszutil

import com.google.cloud.bigquery._
import com.google.cloud.gszutil.Util.{CredentialProvider, Logging}

object MergeInto extends Logging {
  def run(c: Config, cp: CredentialProvider): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, cp.getCredentials)
    val a = TableId.of(c.bqProject, c.bqDataset, c.bqTable)
    val b = TableId.of(c.bqProject2, c.bqDataset2, c.bqTable2)
    val mergeRequest = buildAndValidateMergeRequest(bq, a, b, c.nativeKeyColumns)

    val query = genMerge(mergeRequest)
    logger.debug("Generated SQL:\n" + query)

    val cfg = QueryJobConfiguration.newBuilder(query)
      .setUseLegacySql(false)
      .setAllowLargeResults(true)
      .setUseQueryCache(false)
      .setPriority(QueryJobConfiguration.Priority.BATCH)
      .setDryRun(c.dryRun)
      .build()

    val jobId: JobId = JobId.of(s"gszutil_merge_into_${a.getDataset}_${a.getTable}_${System.currentTimeMillis()/1000}_${Util.randString(6)}")
    BQ.runJob(bq, cfg, jobId, 60*60)
  }

  case class MergeRequest(a: TableId, b: TableId, naturalKeyCols: Seq[String], valCols: Seq[String])

  def getSchema(t: Table): Schema = {
    if (t.getDefinition == null) {
      t.reload()
    }
    t.getDefinition[TableDefinition].getSchema
  }

  def buildAndValidateMergeRequest(bq: BigQuery, a: TableId, b: TableId, naturalKeyCols: Seq[String]): MergeRequest = {
    val tblA = bq.getTable(a)
    val tblB = bq.getTable(b)
    val schemaA = getSchema(tblA)
    val schemaB = getSchema(tblB)
    val fields = getFields(schemaA)
    require(fields.toMap == getFields(schemaB).toMap, "schema mismatch")
    require(naturalKeyCols.forall(fields.contains), "missing key column")
    val valCols = fields.map(_._1).filterNot(naturalKeyCols.contains)
    MergeRequest(a, b, naturalKeyCols, valCols)
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
      .mkString("\n\t\t","\n\tAND\t", "")

    val values = valCols
      .map{colName => s"A.$colName\t=\tB.$colName"}
      .mkString("\n\t","\n\t,","")

    s"""MERGE INTO ${a.getDataset}.${a.getTable} A
       |USING ${b.getDataset}.${b.getTable} B
       |ON $naturalKeys
       |WHEN MATCHED THEN
       |UPDATE SET $values
       |WHEN NOT MATCHED THEN
       |INSERT ROW""".stripMargin
  }
}
