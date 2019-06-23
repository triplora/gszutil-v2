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

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.{GCS, GsUtilConfig}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.orc.WriteORCFile
import com.ibm.jzos.CrossPlatform


object Cp extends Logging {
  def run(c: GsUtilConfig, creds: GoogleCredentials): Result = {
    val copyBook = CrossPlatform.loadCopyBook(c.copyBook)
    val in = CrossPlatform.readChannel(c.source, copyBook)
    val batchSize = (c.blocksPerBatch * in.blkSize) / in.lRecl
    WriteORCFile.run(gcsUri = c.destinationUri,
                     in = in.rc,
                     copyBook = copyBook,
                     gcs = GCS.defaultClient(creds),
                     maxWriters = c.parallelism,
                     batchSize = batchSize,
                     partSizeMb = c.partSizeMB,
                     timeoutMinutes = c.timeOutMinutes,
                     compress = c.compress)
    Result.Success
  }
}
