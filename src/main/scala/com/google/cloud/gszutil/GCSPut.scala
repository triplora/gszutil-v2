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

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZInputStream
import com.google.cloud.pso.ParallelGZIPWriter
import com.google.cloud.storage.Storage.BlobTargetOption
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import com.ibm.jzos.CrossPlatform

import scala.util.Try

object GCSPut extends Logging {
  def run(config: Config, creds: GoogleCredentials): Unit = {
    if (config.parallelism == 1 || !config.compress)
      put(GCS.defaultClient(creds), ZInputStream(config.inDD), config.destBucket, config.destPath, config.compress)
    else {
      ParallelGZIPWriter.run(config.destBucket,
        config.destPath,
        CrossPlatform.readDD(config.inDD),
        GCS.defaultClient(creds),
        config.parallelism)
    }
  }

  def put(gcs: Storage, in: InputStream, bucket: String, path: String, compress: Boolean = true): Util.CopyResult = {
    val blobId = BlobId.of(bucket, if (compress) path + ".gz" else path)
    val w = gcs.writer(BlobInfo.newBuilder(blobId).build())
    logger.info(s"Opened write channel to gs://$bucket/${blobId.getName}")
    val result = Util.transferStreamToChannel(in, w, compress)
    logger.info(s"Finished uploading ${result.bytes} bytes (${result.duration} ms) ${result.fmbps} mb/s md5=${result.hash}")

    // Update blob metadata
    val builder = gcs.get(blobId).toBuilder
      .setMd5(result.hash)
      .setContentType("application/octet-stream")

    if (compress)
      builder.setContentEncoding("gzip")

    Try(builder.build()
      .update(BlobTargetOption.metagenerationMatch()))

    result
  }
}
