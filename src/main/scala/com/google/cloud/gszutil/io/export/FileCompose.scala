package com.google.cloud.gszutil.io.`export`

import java.net.URI

import com.google.cloud.imf.util.Logging
import com.google.cloud.storage.Storage.ComposeRequest
import com.google.cloud.storage.{Blob, BlobInfo, Storage}

trait FileCompose[T, R] {
  def compose(target: T, source: Seq[T]): R
  def composeAll(target: T, sourceDir: T, removeSource: Boolean): R
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

  override def composeAll(target: String, sourceDir: String, removeSource: Boolean): Blob = {
    import scala.jdk.CollectionConverters._
    val sourceUri = new URI(sourceDir)
    val sourceFiles = gcs.list(sourceUri.getAuthority, Storage.BlobListOption.prefix(sourceUri.getPath.stripPrefix("/")))
      .iterateAll.asScala.toSeq
    val targetBlob = compose(target, sourceFiles.map(_.getName))
    if(removeSource) {
      val batch = gcs.batch()
      sourceFiles.foreach(file => batch.delete(file.getBlobId))
      batch.submit()
      logger.info(s"GCS $sourceDir folder with ${sourceFiles.size} files have been removed.")
    }
    targetBlob
  }
}