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
  private var dsnBaseUri: URI = {
    sys.env.get("GCSDSNPREFIX")
      .orElse(sys.env.get("GCSPREFIX"))
      .map(new URI(_)) match {
        case Some(uri) => uri
        case None => null
      }
  }

  private var gdgBaseUri: URI = {
    sys.env.get("GCSGDGPREFIX")
      .map(new URI(_)) match {
      case Some(uri) => uri
      case None => null
    }
  }

  def setBaseUri(uri: String): Unit = {
    dsnBaseUri = new URI(uri)
    if (dsnBaseUri.getScheme != "gs")
      throw new IllegalArgumentException(s"Invalid URI scheme $uri")
  }

  def setBaseGdgUri(uri: String): Unit = {
    gdgBaseUri = new URI(uri)
    if (gdgBaseUri.getScheme != "gs")
      throw new IllegalArgumentException(s"Invalid URIscheme $uri")
  }

  def getBaseUri: URI = dsnBaseUri
  def getBaseGdgUri: URI = gdgBaseUri

  def readCloudDDDSN(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    if (dsnBaseUri == null)
      return None

    CloudLogging.stdout(s"Cloud DD enabled uri=$dsnBaseUri")
    readCloudDDDSN(gcs,dd,ddInfo,dsnBaseUri)
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
    val maybeBlob = Option(gcs.get(BlobId.of(bucket, name)))
    if (maybeBlob.isDefined) {
      CloudLogging.stdout(s"Located Dataset for DD:$dd\n"+
        s"dsn=$dsn\nat gs://$bucket/$name")
      Option(CloudRecordReader(Seq(dsn), ddInfo.lrecl, bucket = bucket, name = name))
    }
    else None
  }

  def readCloudDDGDG(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    if (gdgBaseUri == null)
      return None

    CloudLogging.stdout(s"Generational Cloud DD enabled uri=$gdgBaseUri")
    readCloudDDGDG(gcs,dd,ddInfo,gdgBaseUri)
  }

  def readCloudDDGDG(gcs: Storage, dd: String, ddInfo: DataSetInfo, gdgBaseUri: URI)
  : Option[CloudRecordReader] = {
    val dsn =
      if (!ddInfo.pds) ddInfo.dataSetName
      else ddInfo.dataSetName + "(" + ddInfo.elementName + ")"
    val bucket = gdgBaseUri.getAuthority
    val prefix = gdgBaseUri.getPath.stripPrefix("/").stripSuffix("/")
    val name = if (prefix.nonEmpty) prefix + "/" + dsn else dsn
    // check if DSN exists in GCS
    import scala.jdk.CollectionConverters.IterableHasAsScala
    val versions: IndexedSeq[Blob] = gcs.list(bucket,
      Storage.BlobListOption.prefix(name),
      Storage.BlobListOption.versions(true)).iterateAll.asScala.toIndexedSeq

    if (versions.nonEmpty) {
      CloudLogging.stdout(s"Located Generational Dataset for DD:$dd " +
        s"dsn=$dsn\nuri=gs://$bucket/$name")

      val sb = new StringBuilder
      sb.append(s"$dsn has ${versions.length} generations\n")
      if (versions.length > 1){
        sb.append("generation\tsize\tcreated\n")
        versions.foreach{b =>
          sb.append(b.getGeneration)
          sb.append(" ")
          sb.append(b.getSize)
          sb.append(" ")
          sb.append(StatsUtil.epochMillis2Timestamp(b.getCreateTime))
          sb.append("\n")
        }
      }
      CloudLogging.stdout(sb.result)

      Option(CloudRecordReader(Seq(dsn), ddInfo.lrecl, bucket = bucket, name = name,
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
             |Standard: gs://${r.bucket}/${r.name}
             |GDG: gs://${r1.bucket}/${r.name}
             |""".stripMargin
        logger.error(msg)
        CloudLogging.stderr(msg)
        throw new RuntimeException(msg)
      case (r, None) =>
        r
      case (None, r) =>
        r
    }
  }
}
