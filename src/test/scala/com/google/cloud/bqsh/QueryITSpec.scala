package com.google.cloud.bqsh

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{FieldValueList, JobId, QueryJobConfiguration}
import com.google.cloud.bqsh.cmd.Query
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.Services
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.bp.Duration

class QueryITSpec extends AnyFlatSpec {
  "Query" should "parse stats table" in {
    val example = "test-project-id:TEST_DATASET_A.TABLE_NAME"
    val resolved = BQ.resolveTableSpec(example, "", "")
    assert(resolved.getProject == "test-project-id")
    assert(resolved.getDataset == "TEST_DATASET_A")
    assert(resolved.getTable == "TABLE_NAME")
  }

  it should "query" in {
    val projectId = sys.env("PROJECT_ID")
    val bq = Services.bigQuery(projectId, sys.env.getOrElse("LOCATION", "US"),
      Services.bigqueryCredentials())

    // create a table with one column of each type
    val sql1 =
      s"""create or replace table $projectId.dataset.tbl1 as
        |SELECT
        | 1 as a,
        | 'a' as b,
        | NUMERIC '-3.14'as c,
        | RB"abc+" as d,
        | TIMESTAMP '2014-09-27 12:30:00.45-08'as e,
        | CAST(TIMESTAMP '2014-09-27 12:30:00.45-08' AS STRING) as e1,
        | EXTRACT(DATE FROM CURRENT_TIMESTAMP()) as e2,
        | CURRENT_DATE() as f,
        | 123.456e-67 as g
        |""".stripMargin
    val id1 = JobId.newBuilder().setProject(sys.env("PROJECT_ID"))
      .setLocation(sys.env.getOrElse("LOCATION", "US")).setRandomJob().build()
    bq.query(QueryJobConfiguration.newBuilder(sql1)
      .setUseLegacySql(false).build(), id1)
    val job1 = bq.getJob(id1)
    job1.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(2)))

    val sql =
      s"""select *
        |from $projectId.dataset.tbl1
        |limit 200
        |""".stripMargin
    val id = JobId.newBuilder().setProject(sys.env("PROJECT_ID"))
      .setLocation(sys.env.getOrElse("LOCATION", "US")).setRandomJob().build()
    val result = bq.query(QueryJobConfiguration.newBuilder(sql)
        .setUseLegacySql(false).build(), id)
    val job = bq.getJob(id)
    // wait for job to complete
    job.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(2)))

    val stats = job.getStatistics[QueryStatistics]
    val conf = job.getConfiguration[QueryJobConfiguration]
    val destTable = conf.getDestinationTable

    import scala.jdk.CollectionConverters.IterableHasAsScala

    val rows: Iterable[FieldValueList] = result.iterateAll().asScala

    val schema = result.getSchema.getFields
    val rowCount = result.getTotalRows
    val cols = schema.size()

    rows.foreach{row =>
      val size = row.size()
      // set breakpoint here to inspect result set in debugger
      assert(cols != size, s"schema cols $cols != $size row size")
    }
  }

  it should "print select stats" in {
    val zos = Util.zProvider
    zos.init()
    val projectId = sys.env("PROJECT_ID")
    val dataset = sys.env.getOrElse("DATASET","dataset")

    Query.run(QueryConfig(
      projectId = projectId,
      sql =
        s"""CREATE OR REPLACE TABLE `$projectId.$dataset.UPSERT01` (
           |   ITEM_NBR INT64
           |  ,YR_WK INT64
           |  ,QTY INT64
           |  ,AMT NUMERIC
           |);""".stripMargin
    ), zos, Map.empty)

    Query.run(QueryConfig(
      projectId = projectId,
      destinationTable = s"$projectId.$dataset.UPSERT01",
      replace = true,
      sql =
        s"""SELECT
           | 1 as ITEM_NBR,
           | 1 as YR_WK,
           | 1 as QTY,
           | NUMERIC '1.01'as AMT""".stripMargin
    ), zos, Map.empty)
  }

  it should "print merge stats" in {
    val zos = Util.zProvider
    zos.init()
    val projectId = sys.env("PROJECT_ID")
    val dataset = sys.env.getOrElse("DATASET","dataset")

    Query.run(QueryConfig(
      projectId = projectId,
      sql =
      s"""CREATE OR REPLACE TABLE `$projectId.$dataset.ITEM_DLY_POS` (
         |   ITEM_NBR INT64
         |  ,YR_WK INT64
         |  ,QTY INT64
         |  ,AMT NUMERIC
         |);
         |CREATE OR REPLACE TABLE `$projectId.$dataset.UPSERT01` (
         |   ITEM_NBR INT64
         |  ,YR_WK INT64
         |  ,QTY INT64
         |  ,AMT NUMERIC
         |);
         |INSERT INTO `$projectId.$dataset.ITEM_DLY_POS`
         |(ITEM_NBR,YR_WK,QTY,AMT)
         |VALUES
         |  (1,1,1,NUMERIC '1.01')
         | ,(0,1,1,NUMERIC '1.01')
         |;
         |INSERT INTO `$projectId.$dataset.UPSERT01`
         |(ITEM_NBR,YR_WK,QTY,AMT)
         |VALUES
         |  (1,1,1,NUMERIC '1.01')
         | ,(2,1,1,NUMERIC '1.01')
         |;""".stripMargin
    ), zos, Map.empty)

    Query.run(QueryConfig(
      projectId = projectId,
      sql = s"""MERGE INTO `$projectId.$dataset.ITEM_DLY_POS` D
               |USING `$projectId.$dataset.UPSERT01` S
               |ON D.ITEM_NBR = S.ITEM_NBR
               |  AND D.YR_WK = S.YR_WK
               |WHEN NOT MATCHED THEN
               | INSERT (  ITEM_NBR,   YR_WK,   QTY,   AMT)
               | VALUES (S.ITEM_NBR ,S.YR_WK ,S.QTY ,S.AMT)
               |WHEN MATCHED THEN UPDATE
               |SET D.QTY = S.QTY
               |   ,D.AMT = S.AMT
               |;""".stripMargin
    ), zos, Map.empty)
  }

  it should "print merge stats for aggregate" in {
    val zos = Util.zProvider
    zos.init()
    val projectId = sys.env("PROJECT_ID")
    val dataset = sys.env.getOrElse("DATASET","dataset")

    Query.run(QueryConfig(
      projectId = projectId,
      sql =
        s"""CREATE OR REPLACE TABLE `$projectId.$dataset.ITEM_DLY_POS` (
           |   ITEM_NBR INT64
           |  ,YR_WK INT64
           |  ,QTY INT64
           |  ,AMT NUMERIC
           |);
           |CREATE OR REPLACE TABLE `$projectId.$dataset.UPSERT01` (
           |   ITEM_NBR INT64
           |  ,YR_WK INT64
           |  ,QTY INT64
           |  ,AMT NUMERIC
           |);
           |INSERT INTO `$projectId.$dataset.ITEM_DLY_POS`
           |(ITEM_NBR,YR_WK,QTY,AMT)
           |VALUES
           |  (1,1,1,NUMERIC '1.01')
           | ,(0,1,1,NUMERIC '1.01')
           |;
           |INSERT INTO `$projectId.$dataset.UPSERT01`
           |(ITEM_NBR,YR_WK,QTY,AMT)
           |VALUES
           |  (1,1,1,NUMERIC '1.01')
           | ,(1,1,1,NUMERIC '1.01')
           | ,(2,1,1,NUMERIC '1.01')
           |;""".stripMargin
    ), zos, Map.empty)

    Query.run(QueryConfig(
      projectId = projectId,
      sql = s"""MERGE INTO `$projectId.$dataset.ITEM_DLY_POS` D
               |USING (
               |  SELECT
               |     ITEM_NBR
               |    ,YR_WK
               |    ,SUM(QTY) AS QTY
               |    ,SUM(AMT) AS AMT
               |  FROM `$projectId.$dataset.UPSERT01`
               |  GROUP BY ITEM_NBR, YR_WK
               |) S
               |ON D.ITEM_NBR = S.ITEM_NBR
               |  AND D.YR_WK = S.YR_WK
               |WHEN NOT MATCHED THEN
               | INSERT (  ITEM_NBR,   YR_WK,   QTY,   AMT)
               | VALUES (S.ITEM_NBR, S.YR_WK, S.QTY, S.AMT)
               |WHEN MATCHED THEN UPDATE
               |SET
               |   D.QTY = S.QTY
               |  ,D.AMT = S.AMT""".stripMargin
    ), zos, Map.empty)
  }
}
