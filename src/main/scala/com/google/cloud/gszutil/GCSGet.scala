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

package com.google.cloud.gszutil

import java.nio.channels.FileChannel
import java.nio.file.Paths

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.Storage

object GCSGet extends Logging {
  def run(config: Config, creds: GoogleCredentials): Unit = {
    val gcs = GCS.defaultClient(creds)
    logger.info(s"Downloading gs://${config.srcBucket}/${config.srcPath} to ${config.destPath}")

    get(gcs, config.destBucket, config.destPath)
    logger.info(s"Upload Finished")
  }

  def get(gcs: Storage, bucket: String, path: String): Unit = {
    val rc = gcs.reader(bucket, path)
    val startTime = System.currentTimeMillis()
    Util.transfer(rc, FileChannel.open(Paths.get(path)))
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000L
    logger.info(s"Success ($duration seconds)")
  }
}
