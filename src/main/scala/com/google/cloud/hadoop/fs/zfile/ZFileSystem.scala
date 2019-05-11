package com.google.cloud.hadoop.fs.zfile

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.channels.Channels

import com.google.cloud.gszutil.Util.DebugLogging
import com.google.cloud.gszutil.ZOS
import com.google.cloud.gszutil.io.ZChannel
import com.ibm.jzos.ZFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkConf

object ZFileSystem {
  val FsImpl = "fs.zfile.impl"
  val ClassName = "com.google.cloud.hadoop.fs.zfile.ZFileSystem"

  def sparkConf(c: SparkConf = new SparkConf()): SparkConf =
    c.set(FsImpl, ClassName)

  def configure(c: Configuration = new Configuration()): Configuration = {
    c.set(FsImpl, ClassName)
    c
  }
}

class ZFileSystem extends FileSystem with DebugLogging {
  override def getUri: URI = new URI(s"$getScheme://DD/")

  override def getScheme: String = "zfile"

  override def listStatus(f: Path): Array[FileStatus] = {
    logger.debug(s"listStatus($f)")
    Array(new FileStatus(1, false, 1, 65536, System.currentTimeMillis() - 10000L, f))
  }

  override def getFileStatus(f: Path): FileStatus = {
    logger.debug(s"getFileStatus $f")
    val t = System.currentTimeMillis()
    if (System.getProperty("java.vm.vendor").contains("IBM")) {
      if (ZFile.ddExists(f.getName)) {
        new FileStatus(0, false, 0,
          0, t, t,
          FsPermission.createImmutable(755), "nobody", "nobody", f)
      } else new FileStatus(1, false, 1, 65536, System.currentTimeMillis() - 10000L, f)
    } else {
      new FileStatus(1, false, 1, 65536, System.currentTimeMillis() - 10000L, new Path("/"))
    }
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    if (System.getProperty("java.vm.vendor").contains("IBM")) {
      logger.debug(s"Opening $f")
      val recordReader = ZOS.readDD(f.getName)
      val channel = new ZChannel(recordReader)
      val inputStream = Channels.newInputStream(channel)
      new FSDataInputStream(inputStream)
    } else {
      System.out.println(s"opening $f")
      new FSDataInputStream(new ByteArrayInputStream(Array.empty[Byte]))
    }
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    logger.info(s"create $f")
    null
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    logger.info(s"append $f")
    null
  }

  override def rename(src: Path, dst: Path): Boolean = {
    logger.info(s"rename $src $dst")
    true
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    logger.info(s"delete $f")
    true
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    logger.debug(s"setWorkingDirectory($new_dir")
  }

  override def getWorkingDirectory: Path = new Path("zfile://DD/")

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    logger.info(s"mkdirs $f")
    true
  }
}
