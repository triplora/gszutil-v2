package com.google.cloud.bqsh

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{FieldValueList, JobId, QueryJobConfiguration}
import com.google.cloud.bqsh.cmd.Query
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.{CloudLogging, Services}
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.bp.Duration

class QuerySpec extends AnyFlatSpec {
  CloudLogging.configureLogging(debugOverride = false)
  "Query" should "parse stats table" in {
    val example = "test-project-id:TEST_DATASET_A.TABLE_NAME"
    val resolved = BQ.resolveTableSpec(example,"","")
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
    System.out.println(destTable)
    System.out.println(stats)

    import scala.jdk.CollectionConverters.IterableHasAsScala

    val rows: Iterable[FieldValueList] = result.iterateAll().asScala

    val schema = result.getSchema.getFields
    val rowCount = result.getTotalRows
    val cols = schema.size()

    rows.foreach{row =>
      val size = row.size()
      if (cols != size) System.out.println(s"schema cols $cols != $size row size")
      val fields = (0 until cols).map{i => (i,schema.get(i),row.get(i))}
      fields.foreach{f =>
        System.out.println(f)
        f._3.getValue match {
          case s: String =>
            System.out.println(s"${f._2.getName} $s")
          case x =>
            if (x != null)
              System.out.println(s"${f._1} ${f._2.getName} $x ${x.getClass.getSimpleName}")
            else
              System.out.println(s"${f._1} ${f._2.getName} $x")
        }

      }
    }
  }

  it should "print merge stats" in {
    val zos = Util.zProvider
    zos.init()
    val projectId = sys.env("PROJECT_ID")
    val dataset = sys.env.getOrElse("DATASET","dataset")

    Query.run(QueryConfig(
      projectId = projectId,
      sql =
      s"""CREATE OR REPLACE TABLE `$projectId.$dataset.UPSERT01` (
         |    ITEM_NBR INT64
         |    ,WM_YR_WK INT64
         |    ,STORE_CNT INT64
         |    ,QTY INT64
         |    ,AMT NUMERIC
         |);
         |
         |INSERT INTO `$projectId.$dataset.UPSERT01` (ITEM_NBR,WM_YR_WK,STORE_CNT,QTY,AMT)
         |VALUES
         |(10,16,9,333,NUMERIC '333.72')
         |,(10,17,9,333,NUMERIC '333.72')
         |,(10,18,9,333,NUMERIC '333.72');
         |
         |CREATE OR REPLACE TABLE `$projectId.$dataset.ITEM_DLY_POS` as
         |SELECT
         | 1 as ITEM_NBR,
         | 2 as WM_YR_WK,
         | 3 as STORE_CNT,
         | 4 as QTY,
         | NUMERIC '3.14'as AMT
         |""".stripMargin
    ), zos)

    Query.run(QueryConfig(
      projectId = projectId,
      sql = s"""MERGE INTO `$projectId.$dataset.ITEM_DLY_POS` D
               |USING `$projectId.$dataset.UPSERT01` S
               |ON D.ITEM_NBR = S.ITEM_NBR
               |    AND D.WM_YR_WK = S.WM_YR_WK
               |WHEN NOT MATCHED THEN INSERT (
               |    ITEM_NBR
               |    ,WM_YR_WK
               |    ,STORE_CNT
               |    ,QTY
               |    ,AMT
               |) VALUES (
               |    S.ITEM_NBR
               |    ,S.WM_YR_WK
               |    ,S.STORE_CNT
               |    ,S.QTY
               |    ,S.AMT
               |)
               |WHEN MATCHED THEN UPDATE
               |SET D.STORE_CNT = S.STORE_CNT,D.QTY = S.QTY,D.AMT = S.AMT""".stripMargin
    ), zos)
  }
}
