package com.google.cloud.gszutil

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.hadoop.fs.zfile.ZFileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class DataSourceSpec extends FlatSpec with Logging with BeforeAndAfterAll {

  private var session: SparkSession = _
  def getSpark(): SparkSession = session
  override def beforeAll(): Unit = {
    Util.setDebug("com.google.cloud.hadoop.fs.zfile.ZFileSystem")
    Util.setDebug("org.apache.spark.sql.execution.ZFileFormat")
    Util.setDebug("org.apache.spark.sql.execution.datasources")
    Util.setWarn("org.apache.spark.sql.execution.streaming.state")
    Util.setWarn("org.apache.spark.network.netty")
    Util.setWarn("org.apache.spark.sql.execution.datasources.FileSourceStrategy")
    Util.setWarn("org.apache.orc.impl.MemoryManagerImpl")
    Util.setWarn("org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    Util.setOff("org.apache.hadoop.util.NativeCodeLoader")
    Util.setWarn("org.apache.spark.ui")
    Util.setWarn("org.apache.spark.executor.Executor")
    Util.setWarn("org.apache.spark.SparkEnv")
    Util.setWarn("org.apache.spark.util")
    Util.setWarn("org.apache.spark.scheduler")
    Util.setWarn("org.apache.spark.sql.catalyst.expressions.codegen")
    Util.setWarn("org.apache.spark.SecurityManager")
    Util.setWarn("org.apache.spark.storage.BlockManager")
    Util.setWarn("org.apache.spark.storage.DiskBlockManager")
    Util.setWarn("org.apache.spark.storage.BlockManagerMasterEndpoint")
    Util.setWarn("org.apache.spark.storage.BlockManagerInfo")
    Util.setWarn("org.apache.spark.storage.BlockManagerMaster")
    Util.setWarn("org.apache.spark.storage.memory.MemoryStore")
    Util.setWarn("org.apache.spark.sql.internal.SharedState")
    session = SparkSession.builder()
      .config(ZFileSystem.sparkConf())
      .master("local[1]")
      .getOrCreate()
  }

  "ZFileFormat" should "read" in {
    val spark = getSpark()
    val copyBook = CopyBook(Util.readS("imsku.cpy"))
    val df = spark.read
      .format("zfile")
      .schema(copyBook.getSchema)
      .option("inferSchema", "false")
      .option("copybook", copyBook.raw)
      .load("zfile://DD/INFILE")

    df.write
      .format("orc")
      .option("orc.compress", "none")
      .mode(SaveMode.Overwrite)
      .save("tmp/imsku.orc")
  }

}
