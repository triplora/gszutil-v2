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

class ConfigSpec extends FlatSpec {
  "BQZ" should "parse command" in {
    val args = """bq load --project_id=project --dataset_id=dataset --location=EU --source_format=ORC project:dataset.table gs://mybucket/00/*.orc,gs://mybucket/01/*.orc"""

    val parsed = BqshParser.parse(args.split(" "))
    assert(parsed.isDefined)

    parsed match {
      case Some(c) =>
        assert(c.name == "bq")
        val opts = LoadOptionParser.parse(c.args.drop(1))
        assert(opts.isDefined)
        opts match {
          case Some(x) =>
            assert(x.location == "EU")
            assert(x.source_format == "ORC")
            assert(x.tablespec == "project:dataset.table")
            assert(x.projectId == "project")
            assert(x.datasetId == "dataset")
            assert(x.path == Seq("gs://mybucket/00/*.orc","gs://mybucket/01/*.orc"))
          case _ =>
        }
      case _ =>
    }
  }

}
