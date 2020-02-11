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

import akka.io.BufferPool
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path

/**
  *
  * @param schemaProvider SchemaProvider
  * @param maxBytes number of bytes to accept before closing the writer
  * @param batchSize records per batch
  * @param path Hadoop FileSystem Path gs://bucket/object
  * @param gcs Storage client
  * @param compress if set, compression will be used
  * @param compressBuffer size of compression buffer
  * @param pool BufferPool
  * @param maxErrorPct proportion of acceptable row decoding errors
  */
case class ORCFileWriterArgs(schemaProvider: SchemaProvider, maxBytes: Long, batchSize: Int,
                             path: Path,
                             gcs: Storage, compress: Boolean, compressBuffer: Int, pool: BufferPool,
                             maxErrorPct: Double)
