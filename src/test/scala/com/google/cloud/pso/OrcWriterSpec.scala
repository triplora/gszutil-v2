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

package com.google.cloud.pso

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import com.google.cloud.gszutil.Util.{DefaultCredentialProvider, Logging}
import com.google.cloud.gszutil._
import com.google.cloud.gszutil.orc.WriteORCFile
import org.scalatest.FlatSpec

class OrcWriterSpec extends FlatSpec with Logging {
  Util.configureLogging()
  "OrcWriter" should "write" in {
    val cp = new DefaultCredentialProvider
    val c = Config(
      bqProject = "retail-poc-demo",
      bqBucket = "kms-demo1",
      bqPath = "sku_dly_pos.orc"
    )

    val gcs = GCS.defaultClient(cp.getCredentials)

    val gcsUri = s"gs://${c.bqBucket}/${c.bqPath}"
    val copyBook = CopyBook(Util.readS("sku_dly_pos.cpy"))
    logger.info(s"Loaded copy book```\n${copyBook.raw}\n```")

    val rc = Channels.newChannel(new ByteArrayInputStream(Util.readB("test.bin")))
    WriteORCFile.run(gcsUri,
                     rc,
                     copyBook,
                     gcs,
                     maxWriters = 2,
                     batchSize = 1024,
                     partSizeMb = 128,
                     timeoutMinutes = 3,
                     compress = false)
  }
}
