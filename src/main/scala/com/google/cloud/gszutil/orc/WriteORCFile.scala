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

import java.net.URI
import java.nio.channels.ReadableByteChannel

import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{NonBlockingHeapBufferPool, V2WriterArgs, WriterCore}
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path

object WriteORCFile extends Logging {
  def run(gcsUri: String,
          in: ReadableByteChannel,
          schemaProvider: SchemaProvider,
          gcs: Storage,
          parallelism: Int,
          batchSize: Int,
          partSizeMb: Long,
          timeoutMinutes: Int,
          compressBuffer: Int,
          maxErrorPct: Double): Result = {
    val bufSize = schemaProvider.LRECL * batchSize
    val uri = new URI(gcsUri)
    val basePath = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/")}")

    logger.info("Allocating ByteBuffer pool")
    val pool = new NonBlockingHeapBufferPool(bufSize, 32)
    val actorArgs = V2WriterArgs(
      schemaProvider = schemaProvider,
      partitionBytes = partSizeMb*1024*1024,
      basePath = basePath,
      gcs = gcs,
      pool = pool,
      maxErrorPct = maxErrorPct)

    logger.info(s"Starting $parallelism writers")
    val writers: Array[WriterCore] = (0 until parallelism).toArray.map{i =>
      new WriterCore(actorArgs, s"$i")
    }

    var i = 0
    var records = 0L
    var blocks = 0L
    var n = 1
    var bytesRead = 0L

    logger.info(s"reading")
    while (n >= 0){
      val buf = pool.acquire()
      buf.clear()
      // read until buffer is full
      while (n >= 0 && buf.hasRemaining) {
        // on z/OS this will read a single record
        n = in.read(buf)
        if (n > 0) {
          bytesRead += n
          records += 1
          if (records % 100000 == 0)
            logger.info(s"$records records read")
        }
      }
      if (buf.position > 0) {
        buf.flip
        writers(i).write(buf)
        blocks += 1
        pool.release(buf)
        i += 1
        if (i >= writers.length) i = 0
      }
    }

    logger.info(s"Closing writers")
    writers.foreach(_.close())
    logger.info(s"Upload complete: $records records")
    Result.Success
  }
}
