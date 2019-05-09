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

import com.google.cloud.gszutil.GSXML.{CredentialProvider, XMLStorage}
import com.google.cloud.gszutil.io.ZInputStream

object GCSPut {
  def run(config: Config, cp: CredentialProvider): Unit = {
    val gcs = XMLStorage(cp)
    System.out.println(s"Uploading ${config.inDD} to ${config.dest}")
    put(gcs, ZInputStream(config.inDD), config.destBucket, config.destPath)
    System.out.println(s"Upload Finished")
  }

  def put(gcs: XMLStorage, in: InputStream, bucket: String, path: String): Unit = {
    val request = gcs.putObject(
      bucket = bucket,
      key = path,
      inputStream = in)

    val startTime = System.currentTimeMillis()
    val response = request.execute()
    val endTime = System.currentTimeMillis()

    if (response.isSuccessStatusCode){
      val duration = (endTime - startTime) / 1000L
      System.out.println(s"Success ($duration seconds)")
    } else {
      System.out.println(s"Error: Status code ${response.getStatusCode}\n${response.parseAsString}")
    }
    in.close()
  }
}
