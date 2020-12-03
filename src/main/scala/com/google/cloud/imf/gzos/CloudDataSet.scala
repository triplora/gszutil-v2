/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.imf.gzos

import java.net.URI

import com.google.cloud.gszutil.io.CloudRecordReader
import com.google.cloud.imf.util.{CloudLogging, Logging, StatsUtil}
import com.google.cloud.storage.{Blob, BlobId, Storage}

object CloudDataSet extends Logging {
  private var dsnBaseUri: Option[URI] = {
    sys.env.get("GCSDSNPREFIX")
      .orElse(sys.env.get("GCSPREFIX"))
      .map(new URI(_))
  }

  private var gdgBaseUri: Option[URI] = {
    sys.env.get("GCSGDGPREFIX")
      .map(new URI(_))
  }

  def setBaseUri(uri1: String): Unit = {
    val uri = new URI(uri1)
    if (uri.getScheme != "gs")
      throw new IllegalArgumentException(s"Invalid URI scheme $uri1")
    dsnBaseUri = Option(uri)
  }

  def setBaseGdgUri(uri1: String): Unit = {
    val uri = new URI(uri1)
    if (uri.getScheme != "gs")
      throw new IllegalArgumentException(s"Invalid URI scheme $uri1")
    gdgBaseUri = Option(uri)
  }

  def getBaseUri: Option[URI] = dsnBaseUri
  def getBaseGdgUri: Option[URI] = gdgBaseUri

  def readCloudDDDSN(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    getBaseUri match {
      case Some(baseUri) =>
        CloudLogging.stdout(s"Cloud DD enabled uri=$baseUri")
        readCloudDDDSN(gcs,dd,ddInfo,baseUri)
      case None =>
        None
    }
  }

  def toUri(blob: Blob): String = toUri(blob.getBlobId)

  def toUri(blob: BlobId): String = {
    if (blob.getGeneration != null && blob.getGeneration > 0)
      s"gs://${blob.getBucket}/${blob.getName}#${blob.getGeneration}"
    else
      s"gs://${blob.getBucket}/${blob.getName}"
  }

  def getLrecl(blob: Blob): Int = {
    val uri = toUri(blob)
    val lreclStr = blob.getMetadata.get("lrecl")
    if (lreclStr == null) {
      val msg = s"lrecl not set for $uri"
      CloudLogging.stdout(msg)
      CloudLogging.stderr(msg)
      throw new RuntimeException(msg)
    }
    try {
      Integer.valueOf(lreclStr)
    } catch {
      case _: NumberFormatException =>
        val msg = s"invalid lrecl '$lreclStr' for $uri"
        CloudLogging.stdout(msg)
        CloudLogging.stderr(msg)
        throw new RuntimeException(msg)
    }
  }

  def readCloudDDDSN(gcs: Storage, dd: String, ddInfo: DataSetInfo, dsnBaseUri: URI)
  : Option[CloudRecordReader] = {
    val dsn =
      if (!ddInfo.pds) ddInfo.dataSetName
      else ddInfo.dataSetName + "(" + ddInfo.elementName + ")"
    val bucket = dsnBaseUri.getAuthority
    val prefix = dsnBaseUri.getPath.stripPrefix("/").stripSuffix("/")
    val name = if (prefix.nonEmpty) prefix + "/" + dsn else dsn
    // check if DSN exists in GCS
    val blob: Blob = gcs.get(BlobId.of(bucket, name))
    val uri = toUri(blob)
    if (blob != null) {
      val lrecl: Int = getLrecl(blob)
      CloudLogging.stdout(s"Located Dataset for DD:$dd\n"+
        s"DSN=$dsn\nLRECL=$lrecl\nuri=$uri")
      Option(CloudRecordReader(Seq(dsn), lrecl, bucket = bucket, name = name))
    }
    else None
  }

  def readCloudDDGDG(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    getBaseGdgUri match {
      case Some(baseUri) =>
        CloudLogging.stdout(s"Generational Cloud DD enabled uri=$baseUri")
        readCloudDDGDG(gcs, dd, ddInfo, baseUri)
      case None =>
        None
    }
  }

  def readCloudDDGDG(gcs: Storage, dd: String, ddInfo: DataSetInfo, gdgBaseUri: URI)
  : Option[CloudRecordReader] = {
    val dsn =
      if (!ddInfo.pds) ddInfo.dataSetName
      else ddInfo.dataSetName + "(" + ddInfo.elementName + ")"
    val bucket = gdgBaseUri.getAuthority
    val prefix = gdgBaseUri.getPath.stripPrefix("/").stripSuffix("/")
    val name = if (prefix.nonEmpty) prefix + "/" + dsn else dsn
    val uri = s"gs://$bucket/$name"
    // check if DSN exists in GCS
    import scala.jdk.CollectionConverters.IterableHasAsScala
    val versions: IndexedSeq[Blob] = gcs.list(bucket,
      Storage.BlobListOption.prefix(name),
      Storage.BlobListOption.versions(true))
      .iterateAll.asScala.toIndexedSeq.sortBy(_.getGeneration)

    if (versions.nonEmpty) {
      val sb = new StringBuilder
      sb.append(s"$uri has ${versions.length} generations\n")
      sb.append("generation\tlrecl\tsize\tcreated\n")
      versions.foreach{b =>
        sb.append(b.getGeneration)
        sb.append(" ")
        sb.append(b.getMetadata.get("lrecl"))
        sb.append(" ")
        sb.append(b.getSize)
        sb.append(" ")
        sb.append(StatsUtil.epochMillis2Timestamp(b.getCreateTime))
        sb.append("\n")
      }
      CloudLogging.stdout(sb.result)

      val lrecls = versions.map(getLrecl)
      if (lrecls.distinct.length > 1){
        val msg = s"lrecl inconsistent across generations for $uri - found " +
          s"lrecls ${lrecls.mkString(",")}"
        CloudLogging.stdout(msg)
        CloudLogging.stderr(msg)
        throw new RuntimeException(msg)
      }
      val lrecl = lrecls.head
      CloudLogging.stdout(s"Located Generational Dataset for DD:$dd " +
        s"DSN=$dsn\nLRECL=$lrecl\nuri=$uri")
      Option(CloudRecordReader(Seq(dsn), lrecl, bucket = bucket, name = name,
        gdg = true, generation = ddInfo.elementName, versions = versions))
    }
    else None
  }

  /** Checks GCS for dataset and creates a Record Reader if found
    *
    * @param gcs Storage client instance
    * @param dd DDNAME
    * @param ddInfo DataSetInfo
    * @return Option[CloudRecordReader] - None if data set doesn't exist
    */
  def readCloudDD(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    val cloudReader = readCloudDDDSN(gcs, dd, ddInfo)
    val cloudReaderGdg = readCloudDDGDG(gcs, dd, ddInfo)

    val dsn =
      if (!ddInfo.pds) ddInfo.dataSetName
      else ddInfo.dataSetName + "(" + ddInfo.elementName + ")"

    (cloudReader, cloudReaderGdg) match {
      case (Some(r), Some(r1)) =>
        // throw exception if data set exists in both buckets
        val msg =
          s"""Ambiguous Data Set Reference
             |DD:$dd
             |DSN=$dsn found in both standard and generational buckets
             |Standard: ${r.uri}
             |GDG: ${r1.uri}
             |""".stripMargin
        logger.error(msg)
        CloudLogging.stderr(msg)
        throw new RuntimeException(msg)
      case x =>
        val res = x._1.orElse(x._2)
        if (res.isEmpty)
          logger.info(s"unable to find $dsn in Cloud Storage")
        res
    }
  }
}
