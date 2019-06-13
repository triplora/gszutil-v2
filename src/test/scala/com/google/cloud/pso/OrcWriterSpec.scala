package com.google.cloud.pso

import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.{KeyFileCredentialProvider, Logging}
import com.google.cloud.gszutil.io.{ZChannel, ZDataSet}
import com.google.cloud.gszutil.orc.WriteORCFile
import com.google.cloud.gszutil._
import org.scalatest.FlatSpec

class OrcWriterSpec extends FlatSpec with Logging {
  Util.configureLogging()
  "OrcWriter" should "write" in {
    val cp = KeyFileCredentialProvider(KeyFile.parseFrom(Util.readB("keyfile.pb")))
    val c = Config(
      bqProject = "retail-poc-demo",
      bqBucket = "kms-demo1",
      bqPath = "sku_dly_pos.orc"
    )

    val gcs = GCS.defaultClient(cp.getCredentials)

    val prefix = s"gs://${c.bqBucket}/${c.bqPath}"
    val copyBook = CopyBook(Util.readS("sku_dly_pos.cpy"))
    logger.info(s"Loaded copy book```\n${copyBook.raw}\n```")

    val in = new ZDataSet(Util.readB("test.bin"), copyBook.LRECL, copyBook.LRECL*1024)
    val rc = new ZChannel(in)
    WriteORCFile.run(prefix, rc, copyBook, gcs, maxWriters = 2)
  }
}
