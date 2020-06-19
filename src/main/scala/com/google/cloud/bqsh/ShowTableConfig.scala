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

package com.google.cloud.bqsh

case class ShowTableConfig(
  // Custom Options
  quiet: Boolean = false,

  // Standard Options
  tablespec: String = "",

  // Global Options
  datasetId: String = "",
  location: String = "",
  projectId: String = ""
) {
  def toMap: java.util.Map[String,Any] = {
    val m = new java.util.HashMap[String,Any]()
    m.put("type","ShowTableConfig")
    m.put("tablespec",tablespec)
    m.put("location",location)
    m.put("projectId",projectId)
    m.put("datasetId",datasetId)
    m
  }
}
