package com.google.cloud.pso

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.gszutil.Config.BigQueryConfig
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.{KeyFileCredentialProvider, Logging}
import com.google.cloud.gszutil.io.ZDataSet
import com.google.cloud.gszutil.{Config, Util}
import com.google.cloud.storage.StorageOptions
import org.apache.hadoop.fs.SimpleGCSFileSystem
import org.apache.orc.OrcFile
import org.apache.orc.impl.MemoryManagerImpl
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

    val gcs = StorageOptions.newBuilder()
      .setProjectId(c.bq.project)
      .setCredentials(cp.getCredentials)
      .setRetrySettings(RetrySettings.newBuilder()
        .setMaxAttempts(30)
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRetryDelay(Duration.ofMillis(100))
        .setRetryDelayMultiplier(2.0d)
        .setMaxRetryDelay(Duration.ofMillis(10000))
        .build())
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "gszutil-0.1"))
      .build()
      .getService

    val prefix = s"gs://${c.bq.bucket}/${c.bq.prefix}"
    val copyBookId = sys.env.getOrElse("COPYBOOK", c.copyBook)
    val copyBook = CopyBook(Util.readS(copyBookId))
    logger.info(s"Loaded copy book```\n${copyBook.raw}\n```")

    val conf = SimpleORCWriter.configuration()
    val writerOptions = OrcFile
      .writerOptions(conf)
      .setSchema(copyBook.getOrcSchema)
      .fileSystem(new SimpleGCSFileSystem(gcs))
      .memory(new MemoryManagerImpl(conf))

    val in = new ZDataSet(Util.readB("test.bin"), copyBook.lRecl, copyBook.lRecl*1024)
    //SimpleORCWriter.run(prefix, in, copyBook, writerOptions, maxWriters = 1)
    ParallelORCWriter.run(prefix, in, copyBook, writerOptions, maxWriters = 2)
  }
}
