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

package com.google.cloud.bqsh.cmd

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bqsh.{BQ, RmConfig}

object Rm {
  def run(c: RmConfig, creds: GoogleCredentials): Result = {
    val bq = BQ.defaultClient(c.projectId, c.location, creds)

    if (c.dataset) {
      bq.delete(DatasetId.of(c.projectId, c.datasetId))
    } else if (c.table || c.model) {
      val tableId = BQ.resolveTableSpec(c.tablespec, c.projectId, c.datasetId)
      bq.delete(tableId)
    } else throw new IllegalArgumentException("nothing to delete")

    Result()
  }
}
