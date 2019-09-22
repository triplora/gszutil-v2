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

package com.google.cloud.gszutil.orc

import com.google.common.base.Charsets

object Protocol {
  val Ack: Seq[Byte] = "ACK".getBytes(Charsets.UTF_8).toSeq
  case class PartComplete(path: String, bytesWritten: Long)
  case class FinishedWriting(bytesIn: Long, bytesOut: Long)
  case class PartFailed(msg: String)
  case object Close
  case class UploadComplete(totalBytesRead: Long, totalBytesWritten: Long)
  case object Failed
}
