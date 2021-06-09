package com.google.cloud.bqsh

import com.google.cloud.gszutil.io.exports.StorageFileCompose
import com.google.cloud.imf.util.Services
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

class StorageFileCombinerITSpec extends AnyFlatSpec with BeforeAndAfterEach {

  val TestBucket = sys.env("BUCKET")

  val TargetUrl = s"gs://$TestBucket/TEST"
  val SourceUrl = s"$TargetUrl/tmp"

  val targetUri = new URI(TargetUrl)
  val sourceUri = new URI(SourceUrl)

  val gcs = Services.storage()
  val service = new StorageFileCompose(gcs)

  override protected def beforeEach(): Unit = {
    import scala.jdk.CollectionConverters._
    gcs.list(sourceUri.getAuthority, Storage.BlobListOption.prefix(sourceUri.getPath.stripPrefix("/")))
      .iterateAll.asScala.map(s => s.delete())
    gcs.delete(BlobId.of(targetUri.getAuthority, targetUri.getPath.stripPrefix("/")))
  }

  "GCS file combiner" should "merge all files in folder" in {
    val sources = prepareData()
    val combined = service.composeAll(TargetUrl, SourceUrl)

    assert(combined.exists())
    assert(new String(combined.getContent()) == "Hello World")
    sources.map(s => assert(s.exists()))
  }

  private def prepareData(): Seq[Blob] = {
    val b1 = BlobId.of(sourceUri.getAuthority, sourceUri.getPath.stripPrefix("/").concat("/1"))
    val b2 = BlobId.of(sourceUri.getAuthority, sourceUri.getPath.stripPrefix("/").concat("/2"))
    Seq(createBlob(b1, "Hello "), createBlob(b2, "World"))
  }

  private def createBlob(blobId: BlobId, content: String) = {
    val blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build()
    gcs.create(blobInfo, content.getBytes("UTF-8"))
  }
}
