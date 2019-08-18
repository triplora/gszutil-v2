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

package com.google.cloud.gszutil

import com.google.cloud.bigquery.TableId
import com.google.cloud.bqsh.GsUtilOptionParser
import com.google.cloud.bqsh.op.MergeInto
import com.google.cloud.bqsh.op.MergeInto.MergeRequest
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
}
