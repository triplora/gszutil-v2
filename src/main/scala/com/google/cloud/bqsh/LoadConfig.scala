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

case class LoadConfig(

tablespec: String = "",
path: Seq[String] = Seq.empty,
allow_jagged_rows: Boolean = false,
allow_quoted_newlines: Boolean = false,
autodetect: Boolean = false,
destination_kms_key: String = "",
encoding: String = "",
field_delimiter: String = "",
ignore_unknown_values: Boolean = false,
max_bad_records: Int = -1,
null_marker: String = "",
projection_fields: Seq[String] = Seq.empty,
quote: String = "",
replace: Boolean = false,
append: Boolean = false,
schema: Seq[String] = Seq.empty,
schema_update_option: Seq[String] = Seq.empty,
skip_leading_rows: Long = -1,
source_format: String = "ORC",
clusteringFields: Seq[String] = Seq.empty,
time_partitioning_expiration: Long = -1,
time_partitioning_field: String = "",
time_partitioning_type: String = "",
requirePartitionFilter: Boolean = false,
use_avro_logical_types: Boolean = false,

// Global Options
datasetId: String = "",
debugMode: Boolean = false,
jobId: String = "",
jobProperties: Map[String,String] = Map.empty,
location: String = "US",
projectId: String = "",
synchronousMode: Boolean = true,
sync: Boolean = true
                     )