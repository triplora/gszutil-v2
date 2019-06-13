/*
 * Copyright 2019 Google LLC
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

final case class Config(
                         inDD: String = "INFILE",
                         copyBookDD: String = "COPYBOOK",
                         copyBook: String = "",
                         srcBucket: String = "",
                         srcPath: String = "",
                         dest: String = "",
                         keyfile: String = "",
                         destBucket: String = "",
                         destPath: String = "",
                         mode: String = "",
                         debug: Boolean = false,
                         compress: Boolean = true,
                         batchSize: Int = 10000,
                         partSize: Int = 256*1024*1024,
                         parallelism: Int = 5,
                         bq: BigQueryConfig = BigQueryConfig())