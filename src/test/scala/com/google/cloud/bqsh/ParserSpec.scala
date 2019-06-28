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

package com.google.cloud.bqsh

import org.scalatest.FlatSpec

class ParserSpec extends FlatSpec {
  "MkOptionParser" should "parse" in {
    val args = Seq(
      "bq",
      "mk",
      "--project_id=project",
      "--dataset_id=dataset",
      "--external_table_definition=ORC=gs://bucket/bucket/path.orc/*",
      "TABLE_NAME"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = MkOptionParser.parse(parsed1.get.args)
    assert(parsed.isDefined)
  }

  "LoadOptionParser" should "parse" in {
    val args = Seq(
      "bq",
      "load",
      "--project_id=project",
      "--dataset_id=dataset",
      "--source_format=ORC",
      "TABLE_NAME",
      "gs://bucket/bucket/path.orc/*"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = LoadOptionParser.parse(parsed1.get.args)
    assert(parsed.isDefined)
  }

  "QueryOptionParser" should "parse" in {
    val args = Seq(
      "bq",
      "query",
      "--project_id=project",
      "--dataset_id=dataset",
      "--replace=true",
      "--parameters_from_file=DATE::DDNAME",
      "--destination_table=TABLE_NAME"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = QueryOptionParser.parse(parsed1.get.args.drop(1))
    assert(parsed.isDefined)
  }

  "RmOptionParser" should "parse" in {
    val args = Seq(
      "bq",
      "rm",
      "--project_id=project",
      "--dataset_id=dataset",
      "--table=true",
      "TABLE_NAME"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = RmOptionParser.parse(parsed1.get.args.drop(1))
    assert(parsed.isDefined)
  }

  "GsUtilRmOptionParser" should "parse" in {
    val args = Seq(
      "gsutil",
      "rm",
      "gs://bucket/path"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = GsUtilOptionParser.parse(parsed1.get.args.drop(1))
    assert(parsed.isDefined)
  }

  "GsUtilCpOptionParser" should "parse" in {
    val args = Seq(
      "gsutil",
      "cp",
      "gs://bucket/path"
    )
    val parsed1 = BqshParser.parse(args)
    assert(parsed1.isDefined)
    val parsed = GsUtilOptionParser.parse(parsed1.get.args.drop(1))
    assert(parsed.isDefined)
  }

  "Bqsh" should "split SQL" in {
    val queryString =
      """-- comment
        | SELECT 1 FROM DUAL;
        | SELECT 2 FROM DUAL;
        |""".stripMargin
    val split = Bqsh.splitSQL(queryString)
    val expected = Seq(
      "-- comment\n SELECT 1 FROM DUAL",
      "SELECT 2 FROM DUAL"
    )
    assert(split == expected)
  }
}
