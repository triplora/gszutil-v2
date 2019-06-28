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

import com.google.cloud.gszutil.Decoding.{PicDecimal, PicInt, PicString}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.{CopyBook, Decoding, Util}
import org.scalatest.FlatSpec

class CopyBookSpec extends FlatSpec with Logging {
  "CopyBook" should "parse" in {
    Util.configureLogging(true)
    for (name <- (1 to 4).map(i => s"test$i.cpy")){
      val cb1 = CopyBook(Util.readS(name))
      val s = cb1.Fields.map(_.toString).mkString("\n")
      System.out.println("\n*********************************\n")
      System.out.println(s)
      System.out.println("\n*********************************\n")
    }
  }

  it should "map types" in {
    Seq(
      "PIC S9 COMP." -> PicInt(2),
      "PIC S9(4) COMP." -> PicInt(2),
      "PIC S9(5) COMP." -> PicInt(4),
      "PIC S9(9) COMP." -> PicInt(4),
      "PIC S9(10) COMP." -> PicInt(8),
      "PIC S9(18) COMP." -> PicInt(8),
      "PIC X." -> PicString(1),
      "PIC X(8)." -> PicString(8),
      "PIC X(16)." -> PicString(16),
      "PIC X(30)." -> PicString(30),
      "PIC X(20)." -> PicString(20),
      "PIC X(2)." -> PicString(2),
      "PIC X(10)." -> PicString(10),
      "PIC S9(9)V9(2) COMP-3." -> PicDecimal(9,2),
      "PIC S9(9)V9(3) COMP-3." -> PicDecimal(9,3),
      "PIC S9(3) COMP-3." -> PicDecimal(9,3),
      "PIC S9(7) COMP-3." -> PicDecimal(9,7),
      "PIC S9(9) COMP-3." -> PicDecimal(9,9),
      "PIC S9(9)V99 COMP-3." -> PicDecimal(9,2),
      "PIC S9(6)V99 COMP-3." -> PicDecimal(6,2),
      "PIC S9(13)V99 COMP-3" -> PicDecimal(13,2),
      "PIC S9(7)V99 COMP-3" -> PicDecimal(7,2),
      "PIC S9(7)V999 COMP-3" -> PicDecimal(7,3),
      "PIC S9(16)V9(2) COMP-3" -> PicDecimal(16,2)
    ).foreach{x =>
      assert(Decoding.typeMap(x._1) == x._2)
    }
  }

  it should "trim" in {
    assert(Util.trimRight("abc   ", ' ') == "abc")
    assert(Util.trimRight("   ", ' ') == "")
    assert(Util.trimRight("", ' ') == "")
  }
}
