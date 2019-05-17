package org.apache.hadoop.fs

import java.net.URI
import java.nio.channels.Channels

import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

object SimpleGCSFileSystem {
  val FsImpl = "fs.gs.impl"
  val ClassName = "com.google.cloud.hadoop.fs.gs.SimpleGCSFileSystem"

  def configure(c: Configuration = new Configuration()): Configuration = {
    c.set(FsImpl, ClassName)
    c
  }

  def toBlobId(path: Path): BlobId = {
    val uri = path.toUri
    BlobId.of(uri.getAuthority, uri.getPath.stripPrefix("/"))
  }
}

class SimpleGCSFileSystem(storage: Storage) extends FileSystem {
  import SimpleGCSFileSystem._

  override def getUri: URI = new URI(s"$getScheme://")

  override def getScheme: String = "gs"

  override def open(f: Path, bufferSize: Int): FSDataInputStream = throw new UnsupportedOperationException()

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    val w = storage.writer(BlobInfo.newBuilder(toBlobId(f)).build())
    val os = Channels.newOutputStream(w)
    new FSDataOutputStream(os, null, 0)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = throw new UnsupportedOperationException()

  override def rename(src: Path, dst: Path): Boolean = throw new UnsupportedOperationException()

  override def delete(f: Path, recursive: Boolean): Boolean = throw new UnsupportedOperationException()

  override def listStatus(f: Path): Array[FileStatus] = Array.empty

  override def setWorkingDirectory(new_dir: Path): Unit = {}

  override def getWorkingDirectory: Path = new Path("gs://bucket/")

  override def mkdirs(f: Path, permission: FsPermission): Boolean = throw new UnsupportedOperationException()

  override def getFileStatus(f: Path): FileStatus = throw new UnsupportedOperationException()
}
