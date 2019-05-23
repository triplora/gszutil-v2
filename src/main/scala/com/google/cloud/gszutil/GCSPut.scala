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

import java.io.InputStream
import java.nio.channels.{Channels, ReadableByteChannel}

import com.google.cloud.gszutil.Util.{CredentialProvider, Logging}
import com.google.cloud.gszutil.io.{ZChannel, ZInputStream}
import com.google.cloud.storage.Storage.BlobTargetOption
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}

object GCSPut extends Logging {
  def run(config: Config, cp: CredentialProvider): Unit = {
    val gcs = GCS.defaultClient(cp.getCredentials)
    logger.info(s"Uploading ${config.inDD} to ${config.dest}")
    val result = put(gcs, ZInputStream(config.inDD), config.destBucket, config.destPath)
    logger.info(s"Finished uploading ${result.bytes} bytes (${result.duration} ms) ${result.fmbps} mb/s md5=${result.hash}")
  }

  def put(gcs: Storage, in: InputStream, bucket: String, path: String): Util.CopyResult = {
    val blobId = BlobId.of(bucket,path)
    val w = gcs.writer(BlobInfo.newBuilder(blobId).build())
    val blob = gcs.get(blobId)

    val result = Util.transferWithHash(Channels.newChannel(in), w)

    blob.toBuilder
      .setMd5(result.hash)
      .setContentType("application/octet-stream")
      .build()
      .update(BlobTargetOption.metagenerationMatch())

    result
  }

  def putDD(gcs: Storage, dd: String, destBucket: String, destPath: String): Util.CopyResult =
    putChannel(gcs, ZChannel(dd), destBucket, destPath)

  def putChannel(gcs: Storage, in: ReadableByteChannel, destBucket: String, destPath: String): Util.CopyResult = {
    val out = gcs.writer(BlobInfo.newBuilder(BlobId.of(destBucket, destPath)).build())
    Util.transferWithHash(in, out)
  }
}
