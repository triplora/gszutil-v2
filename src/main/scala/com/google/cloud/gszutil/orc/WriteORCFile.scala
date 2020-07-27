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
import java.nio.ByteBuffer

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.{WriterCore, ZRecordReaderT}
import com.google.cloud.imf.util.Logging
import org.apache.hadoop.fs.Path

object WriteORCFile extends Logging {
  def run(gcsUri: String,
          in: ZRecordReaderT,
          schemaProvider: SchemaProvider,
          cred: OAuth2Credentials,
          parallelism: Int,
          batchSize: Int,
          maxErrorPct: Double): Result = {
    val bufSize = in.lRecl * batchSize
    val buf = ByteBuffer.allocate(bufSize)
    val uri = new URI(gcsUri)
    val basePath = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/")}")

    logger.info(s"Opening $parallelism ORC Writers for $basePath")
    val writers: Array[WriterCore] = (0 until parallelism).toArray.map{i =>
      new WriterCore(schemaProvider = schemaProvider,
        basePath = basePath,
        cred = cred,
        name = s"$i",
        lrecl = in.lRecl)
    }

    var i = 0
    var blocks = 0L
    var n = 1
    var bytesRead = 0L
    var errCount = 0L

    logger.info(s"Reading from DSN:${in.getDsn}")
    while (n >= 0){
      buf.clear()
      // read until buffer is full
      while (n >= 0 && buf.remaining >= in.lRecl) {
        // on z/OS this will read a single record
        n = in.read(buf)
        bytesRead += n
      }
      if (n < 0) bytesRead -= n
      if (buf.position() > 0) {
        buf.flip
        val result = writers(i).write(buf)
        errCount += result.errCount
        blocks += 1
        i += 1
        if (i >= writers.length) i = 0
      }
    }
    val records = bytesRead / in.lRecl

    logger.debug(s"Closing writers")
    writers.foreach(_.close())
    logger.info(s"Finished writing ORC: $records records $blocks blocks $bytesRead bytes")

    val errPct = errCount.toDouble / records.toDouble
    if (errPct > maxErrorPct)
      Result(message = s"error percent $errPct > $maxErrorPct",
        activityCount = records - errCount, exitCode = 1)
    else
      Result(activityCount = records)
  }
}
