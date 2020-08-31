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
import java.util.zip.GZIPOutputStream

import com.google.api.services.storage.StorageScopes
import com.google.cloud.bqsh.{ArgParser, Command, ScpConfig, ScpOptionParser}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.{Logging, Services, StorageObjectOutputStream}
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
      val gcsUri = new URI(config.destGsUri)
      val bucket = gcsUri.getAuthority
      val objName = gcsUri.getPath.stripPrefix("/")

      val in: ZRecordReaderT = zos.readDD(config.sourceDD)
      try {
        val lrecl = in.lRecl

        val creds = zos.getCredentialProvider().getCredentials
          .createScoped(StorageScopes.DEVSTORAGE_READ_WRITE)

        val os: OutputStream =
          new BufferedOutputStream(new GZIPOutputStream(new StorageObjectOutputStream(
            creds.refreshAccessToken(), bucket, objName), 32*1024, true), 1024*1024)

        val buf = new Array[Byte](lrecl)
        var nRecordsRead: Long = 0
        while (in.read(buf) > -1 && nRecordsRead < config.limit){
          os.write(buf, 0, lrecl)
          nRecordsRead += 1
        }
        os.close()
        in.close()
      } catch {
        case t: Throwable =>
          in.close()
          throw t
      }

      val gcs = Services.storage(creds)
      val blob = gcs.update(BlobInfo.newBuilder(BlobId.of(bucket, objName))
        .setMetadata(ImmutableMap.of(
          "content-encoding", "gzip",
          "x-goog-meta-dsn", in.getDsn,
          "x-goog-meta-records", s"$nRecordsRead",
          "x-goog-meta-lrecl", s"$lrecl",
          "x-goog-meta-blksize", s"${in.blkSize}"))
        .build())
      System.out.println(s"Uploaded $blob ")
      Result(activityCount = nRecordsRead)
    } catch {
      case e: Throwable =>
        System.out.println(s"failed to sample data: ${e.getMessage}")
        e.printStackTrace(System.err)
        Result.Failure(e.getMessage)
    }
  }
}
