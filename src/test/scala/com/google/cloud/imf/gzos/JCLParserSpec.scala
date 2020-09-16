/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.imf.gzos

import com.google.cloud.imf.gzos.JCLParser.DDStatement
import org.scalatest.flatspec.AnyFlatSpec

class JCLParserSpec extends AnyFlatSpec {
  "JCL" should "parse" in {
    val example = """\\INFILE DD DSN=HLQ.LOAD1,DISP=SHR,LRECL=123"""
    val stmts = JCLParser.splitStatements(example)
    val expected = List(DDStatement("INFILE",123,List("HLQ.LOAD1")))
    assert(stmts == expected)
  }

  it should "multi" in {
    val example =
      """\\INFILE DD DSN=HLQ.LOAD1,DISP=SHR,LRECL=123
        |\\       DD DSN=HLQ.LOAD2,DISP=SHR""".stripMargin
    val actual = JCLParser.splitStatements(example)
    val expected = List(DDStatement("INFILE",123,List("HLQ.LOAD1","HLQ.LOAD2")))
    assert(actual == expected)
  }

  it should "quoted" in {
    val example = """\\INFILE DD DSN='gs://bucket/prefix/HLQ.LOAD1',LRECL=123"""
    val actual = JCLParser.splitStatements(example)
    val expected = List(DDStatement("INFILE",123,List("gs://bucket/prefix/HLQ.LOAD1")))
    assert(actual == expected)
  }

  it should "continued" in {
    val example =
      """\\INFILE DD DSN='gs://long-bucket-name/long_object_name_prefix/HLQ.LONG.
        |\\             DSN.LOAD1',LRECL=123""".stripMargin
    val actual = JCLParser.splitStatements(example)
    val expected = List(DDStatement("INFILE",123,List("gs://long-bucket-name/long_object_name_prefix/HLQ.LONG.DSN.LOAD1")))
    assert(actual == expected)
  }

  it should "continued 2" in {
    val example =
      """\\INFILE DD DSN=HLQ.DSN.LOAD1,
        |\\          LRECL=41""".stripMargin
    val actual = JCLParser.splitStatements(example)
    val expected = List(DDStatement("INFILE",41,List("HLQ.DSN.LOAD1")))
    assert(actual == expected)
  }
}
