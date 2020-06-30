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

import com.google.common.collect.ImmutableSortedMap

case class RmConfig (
  dataset: Boolean = false,
  model: Boolean = false,
  recursive: Boolean = false,
  table: Boolean = false,
  tablespec: String = "",

  // Global Options
  datasetId: String = "",
  debugMode: Boolean = false,
  jobId: String = "",
  jobProperties: Map[String,String] = Map.empty,
  location: String = "",
  projectId: String = "",
  synchronousMode: Boolean = true,
  sync: Boolean = true
) {
  def toMap: java.util.Map[String,Any] = ImmutableSortedMap.of(
    "type", "RmConfig",
    "tablespec", tablespec,
    "projectId", projectId,
    "location", location,
    "recursive", recursive
  )
}
