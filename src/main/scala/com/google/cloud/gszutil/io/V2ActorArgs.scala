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

package com.google.cloud.gszutil.io

import akka.actor.ActorRef
import akka.io.BufferPool
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path
import org.zeromq.ZMQ.Socket

/**
  *
  * @param socket org.zeromq.ZMQ.Socket
  * @param blkSize block size
  * @param nWriters number of writers
  * @param copyBook CopyBook
  * @param partitionBytes number of bytes per partition
  * @param path org.apache.hadoop.fs.Path gs://bucket/prefix
  * @param gcs Storage client
  * @param compress if set, compression will be used
  * @param pool BufferPool
  * @param maxErrorPct proportion of acceptable row decoding errors
  * @param notifyActor ActorRef to send results
  */
case class V2ActorArgs(socket: Socket, blkSize: Int, nWriters: Int,
                       copyBook: CopyBook, partitionBytes: Long, path: Path,
                       gcs: Storage, compress: Boolean, pool: BufferPool,
                       maxErrorPct: Double, notifyActor: ActorRef)
