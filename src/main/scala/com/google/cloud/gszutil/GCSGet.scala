package com.google.cloud.gszutil

import java.nio.channels.{Channels, FileChannel}
import java.nio.file.{Paths, StandardOpenOption}

import com.google.cloud.gszutil.GSXML.{CredentialProvider, XMLStorage}

object GCSGet {
  def run(config: Config, cp: CredentialProvider): Unit = {
    val gcs = XMLStorage(cp)
    System.out.println(s"Downloading gs://${config.srcBucket}/${config.srcPath} to ${config.destPath}")

    get(gcs, config.destBucket, config.destPath)
    System.out.println(s"Upload Finished")
  }

  def get(gcs: XMLStorage, bucket: String, path: String): Unit = {
    val request = gcs.getObject(bucket, path)

    val startTime = System.currentTimeMillis()
    val response = request.execute()
    val endTime = System.currentTimeMillis()

    import StandardOpenOption._
    val opts = Seq(CREATE, WRITE, TRUNCATE_EXISTING)
    Util.transfer(
      rc = Channels.newChannel(response.getContent),
      wc = FileChannel.open(Paths.get(path), opts:_*)
    )

    if (response.isSuccessStatusCode){
      val duration = (endTime - startTime) / 1000L
      System.out.println(s"Success ($duration seconds)")
    } else {
      System.out.println(s"Error: Status code ${response.getStatusCode}\n${response.parseAsString}")
    }
  }
}
