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
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path
import org.zeromq.ZMQ.Socket

/**
  *
  * @param socket org.zeromq.ZMQ.Socket (reader only)
  * @param blkSize block size (reader only)
  * @param nWriters number of writers
  * @param schemaProvider SchemaProvider
  * @param partitionBytes number of bytes per partition
  * @param basePath GCS URI where parts will be written (gs://bucket/prefix)
  * @param gcs Storage client
  * @param pool BufferPool used to obtain ByteBuffer instances
  * @param maxErrorPct proportion of acceptable row decoding errors
  * @param notifyActor ActorRef to send results (reader only)
  */
case class V2ServerArgs(socket: Socket, // Only for reader
                        blkSize: Int, // Only for reader
                        nWriters: Int, // Only for reader
                        schemaProvider: SchemaProvider,
                        partitionBytes: Long,
                        basePath: Path,
                        gcs: Storage,
                        pool: BufferPool,
                        maxErrorPct: Double,
                        notifyActor: ActorRef // Only for reader
                      ) {
  def writeArgs: V2WriterArgs = V2WriterArgs(
    schemaProvider = schemaProvider,
    partitionBytes = partitionBytes,
    basePath = basePath,
    gcs = gcs,
    pool = pool,
    maxErrorPct = maxErrorPct)
}
