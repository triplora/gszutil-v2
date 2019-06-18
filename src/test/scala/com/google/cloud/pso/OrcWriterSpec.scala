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
