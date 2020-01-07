package com.google.cloud.bqmload

import java.util

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.{BigQuery, InsertAllRequest, TableId}
import com.google.cloud.bqsh.BQ
import com.ibm.jzos.ZFileProvider

object BQMLD {
  def main(args: Array[String]): Unit = {
    val zos = ZFileProvider.getProvider()
    System.out.println("Collecting Job Info")
    val jobInfo = collectJobInfo(zos)
    val project = sys.env.getOrElse("PROJECT", "")
    val bq = BQ.defaultClient(project,
      sys.env.getOrElse("LOCATION", "US"),
      zos.getCredentialProvider().getCredentials)
    val tableId = TableId.of(project,
      sys.env.getOrElse("DATASET", "LOG"),
      sys.env.getOrElse("TABLE", "BQMLD"))

    System.out.println("Inserting Job Info into BigQuery log table")
    insertRow(jobInfo, bq, tableId)
    System.out.println("Done")

    // TODO parse

    // TODO copy data

    // TODO generate DML plan

    // TODO submit bigquery jobs

    // TODO log results
  }

  def insertRow(content: util.Map[String,String],
                bq: BigQuery,
                tableId: TableId): Unit = {
    val request = InsertAllRequest.of(tableId, RowToInsert.of(content))
    val result = bq.insertAll(request)
    if (result.hasErrors) {
      import scala.collection.JavaConverters.collectionAsScalaIterableConverter
      val errors = result.getInsertErrors.values().asScala.flatMap(_.asScala)
      System.err.println("BigQuery Insert errors:")
      for (e <- errors)
        System.err.println(s"${e.getMessage}")
    }
  }

  def collectJobInfo(zos: ZFileProvider): util.Map[String,String] = {
    val info = zos.getInfo
    val script = zos.readStdin()
    val substituted = zos.substituteSystemSymbols(script)
    val content = new util.HashMap[String,String]()
    content.put("jobid", zos.jobId)
    content.put("jobdate", zos.jobDate)
    content.put("jobtime", zos.jobTime)
    content.put("jobname", zos.jobName)
    content.put("stepname", info.stepName)
    content.put("user", info.user)
    if (!substituted.contentEquals(script)) {
      content.put("script", substituted)
      content.put("template", script)
    } else {
      content.put("script", script)
    }
    content
  }
}
