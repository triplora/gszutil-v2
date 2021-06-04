package com.google.cloud.gszutil.io.exports

import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage.ComposeRequest
import com.google.cloud.storage.{Blob, BlobInfo, Storage}

import java.net.URI

trait FileCompose[T, R] {
  def compose(target: T, source: Seq[T]): R

  def composeAll(target: T, sourceDir: T): R
}

class StorageFileCompose(gcs: Storage) extends FileCompose[String, Blob] with Logging {

  override def compose(target: String, source: Seq[String]): Blob = {
    import scala.jdk.CollectionConverters._
    val targetUri = new URI(target)
    val request = ComposeRequest.newBuilder()
      .addSource(source.asJava)
      .setTarget(BlobInfo.newBuilder(targetUri.getAuthority, targetUri.getPath.stripPrefix("/")).build()).build()
    val res = gcs.compose(request)
    logger.info(s"Files [$source] have been composed into $target.")
    res
  }

  override def composeAll(target: String, sourceDir: String): Blob = {
    import scala.jdk.CollectionConverters._
    val sourceUri = new URI(sourceDir)
    val sourceFiles = gcs.list(sourceUri.getAuthority, Storage.BlobListOption.prefix(sourceUri.getPath.stripPrefix("/")))
      .iterateAll.asScala.toSeq
    compose(target, sourceFiles.map(_.getName))
  }
}