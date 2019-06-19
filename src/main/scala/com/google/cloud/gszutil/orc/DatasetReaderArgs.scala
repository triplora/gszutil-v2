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

package com.google.cloud.gszutil.orc

import java.net.URI
import java.nio.channels.ReadableByteChannel

import akka.io.BufferPool
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.storage.Storage

/**
  *
  * @param in input Data Set
  * @param batchSize rows per batch
  * @param uri prefix URI
  * @param maxBytes input bytes per part
  * @param nWorkers worker count
  * @param copyBook CopyBook
  */
case class DatasetReaderArgs(in: ReadableByteChannel, batchSize: Int, uri: URI, maxBytes: Long, nWorkers: Int, copyBook: CopyBook, gcs: Storage, compress: Boolean, pool: BufferPool)
