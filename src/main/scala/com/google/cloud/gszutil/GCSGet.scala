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
