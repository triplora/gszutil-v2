package com.google.cloud.gszutil

import com.google.cloud.bigquery.TableId
import com.google.cloud.gszutil.MergeInto.MergeRequest
import org.scalatest.FlatSpec

class MergeSpec extends FlatSpec {
  "MergeInto" should "validate merge" in {
    val fields = Map[String,String](
      "a" -> "a",
      "b" -> "b",
      "c" -> "c",
      "d" -> "d"
    )
    val fields2 = fields
    val fields3 = fields ++ Seq(("e", "e"))
    val naturalKeyCols: Seq[String] = Seq("a","b")
    val naturalKeyCols2: Seq[String] = Seq("a","b","c","d")
    assert(MergeInto.validateMerge(fields, fields2, naturalKeyCols.toSet))
    assertThrows[IllegalArgumentException](MergeInto.validateMerge(fields, fields3, naturalKeyCols.toSet))
    assertThrows[IllegalArgumentException](MergeInto.validateMerge(fields, fields3, naturalKeyCols2.toSet))
  }

  it should "generate sql" in {
    val req = MergeRequest(
      TableId.of("project","dataset", "table"),
      TableId.of("project","dataset","table"),
      Seq("a","b"),
      Seq("c","d")
    )

    val sql = MergeInto.genMerge(req)
    val expected =
      """MERGE INTO `project.dataset.table` A
        |USING `project.dataset.table` B
        |ON
        |		A.a	=	B.a
        |	AND	A.b	=	B.b
        |WHEN MATCHED THEN
        |UPDATE SET
        |	A.c	=	B.c
        |	,A.d	=	B.d
        |WHEN NOT MATCHED THEN
        |INSERT ROW""".stripMargin
    assert(sql == expected)
  }

  it should "parse args" in {
    val args = "merge destProject destDataset destTable srcProject srcDataset srcTable a,b,c"
    val config = ConfigParser.parse(args.split(" "))
    assert(config.isDefined)
    config match {
      case Some(c) =>
        assert(c.mode == "merge")
        assert(c.nativeKeyColumns == Seq("a","b","c"))
    }
  }

}
