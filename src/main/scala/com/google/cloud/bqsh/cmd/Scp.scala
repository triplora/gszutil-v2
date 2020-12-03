/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

import java.io.{BufferedOutputStream, OutputStream}
import java.net.URI
import java.nio.channels.Channels
import java.util.zip.GZIPOutputStream

import com.google.api.services.storage.StorageScopes
import com.google.cloud.bqsh.{ArgParser, Command, ScpConfig, ScpOptionParser}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.{MVS, MVSStorage}
import com.google.cloud.imf.util.{CloudLogging, Logging, Services}
import com.google.cloud.storage.{BlobId, BlobInfo}
import com.google.common.collect.ImmutableMap

/** Simple binary copy
  * Single connection - not recommended for large uploads
  * Writes gzip compressed object
  */
object Scp extends Command[ScpConfig] with Logging {
  override val name: String = "scp"
  override val parser: ArgParser[ScpConfig] = ScpOptionParser
  override def run(config: ScpConfig, zos: MVS): Result = {
    try {
      val gcsUri = new URI(config.outUri)
      val bucket = gcsUri.getAuthority
      val objName = gcsUri.getPath.stripPrefix("/")
      val creds = zos.getCredentialProvider().getCredentials
        .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)
      val gcs = Services.storage(creds)

      val in: ZRecordReaderT = zos.readDSN(MVSStorage.parseDSN(config.inDsn))
      val lrecl = in.lRecl

      // include lrecl and gzip in object metadata
      val b = ImmutableMap.builder[String,String]()
      if (config.compress)
        b.put("content-encoding", "gzip")
      b.put("x-goog-meta-lrecl", s"$lrecl")
      val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, objName))
        .setMetadata(b.build).build

      val os: OutputStream = {
        val out = gcs.writer(blobInfo)
        val o = Channels.newOutputStream(out)
        if (config.compress)
          new BufferedOutputStream(new GZIPOutputStream(o), 256*1024)
        else
          new BufferedOutputStream(o, 256*1024)
      }

      var nRecordsRead: Long = 0
      var n = 0
      try {
        val buf = new Array[Byte](lrecl)
        n = in.read(buf)
        while (n > -1 && nRecordsRead < config.limit){
          if (n > 0) {
            nRecordsRead += 1
            os.write(buf, 0, lrecl)
          }
          n = in.read(buf)
        }
        os.close()
        in.close()
      } catch {
        case t: Throwable =>
          in.close()
          val msg = s"exception during upload: ${t.getMessage}"
          CloudLogging.stdout(msg)
          CloudLogging.stderr(msg)
          CloudLogging.printStackTrace(t)
          throw t
      }

      CloudLogging.stdout(s"Uploaded $nRecordsRead to ${config.outUri}")
      Result(activityCount = nRecordsRead)
    } catch {
      case e: Throwable =>
        val msg = s"failed to sample data: ${e.getMessage}"
        CloudLogging.stdout(msg)
        CloudLogging.printStackTrace(e)
        Result.Failure(msg)
    }
  }
}
