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

package com.google.cloud.bqsh.cmd

import java.net.URI

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.{GCS, GsUtilConfig}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.{BlobId, Storage}

object GsUtilRm extends Logging {
  def run(c: GsUtilConfig, creds: GoogleCredentials): Result = {
    val gcs = GCS.defaultClient(creds)
    val uri = new URI(c.destinationUri)
    val bucket = uri.getAuthority
    if (c.recursive) {
      var ls = gcs.list(bucket, Storage.BlobListOption.prefix(c.destinationUri))
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter
      var deleted: Long = 0
      var notDeleted: Long = 0
      while (ls.hasNextPage) {
        val blobIds = ls.getValues.asScala.toArray.map(_.getBlobId)
        val deleteResults = gcs.delete(blobIds: _*)
        deleted += deleteResults.asScala.count(_ == true)
        notDeleted += deleteResults.asScala.count(_ == false)
        ls = ls.getNextPage
      }
      logger.info(s"$deleted deleted $notDeleted notDeleted")
      Result.withExportLong("ACTIVITYCOUNT", deleted)
    } else {
      val deleted = gcs.delete(BlobId.of(bucket, uri.getPath.stripPrefix("/")))
      if (deleted) {
        logger.info(s"deleted $uri")
        Result.withExportLong("ACTIVITYCOUNT", 1)
      } else {
        logger.info(s"$uri was not deleted")
        Result.withExportLong("ACTIVITYCOUNT", 0)
      }
    }
  }
}
