package com.google.cloud.pso

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.gszutil.Config.BigQueryConfig
import com.google.cloud.gszutil.{Config, CopyBook, GCS, Util}
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.{KeyFileCredentialProvider, Logging}
import com.google.cloud.gszutil.io.ZDataSet
import com.google.cloud.storage.StorageOptions
import org.scalatest.FlatSpec
import org.threeten.bp.Duration

class OrcWriterSpec extends FlatSpec with Logging {
  Util.configureLogging()
  "OrcWriter" should "write" in {
    val cp = KeyFileCredentialProvider(KeyFile.parseFrom(Util.readB("keyfile.pb")))
    val c: Config = Config(
      bq = BigQueryConfig(
        project = "retail-poc-demo",
        bucket = "kms-demo1",
        prefix = "sku_dly_pos.orc"
      ), copyBook = "sku_dly_pos.cpy"
    )

    val gcs = GCS.defaultClient(cp.getCredentials)

    val prefix = s"gs://${c.bq.bucket}/${c.bq.prefix}"
    val copyBookId = sys.env.getOrElse("COPYBOOK", c.copyBook)
    val copyBook = CopyBook(Util.readS(copyBookId))
    logger.info(s"Loaded copy book```\n${copyBook.raw}\n```")

    val in = new ZDataSet(Util.readB("test.bin"), copyBook.lRecl, copyBook.lRecl*1024)
    ParallelORCWriter.run(prefix, in, copyBook, gcs, maxWriters = 2)
  }
}
