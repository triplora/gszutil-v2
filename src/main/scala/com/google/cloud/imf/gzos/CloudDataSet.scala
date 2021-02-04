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
import com.google.cloud.imf.util.{Logging, StatsUtil}
import com.google.cloud.storage.{Blob, BlobId, Storage}
import com.google.cloud.imf.util.CloudDataSetUtil._

/**  add these lines to environment file:
  *  export GCSDSNURI=gs://bucket/prefix
  *  export GCSGDGURI=gs://bucket-with-versioning/prefix
  */
object CloudDataSet extends Logging {
  val DsnVar = "GCSDSNURI"
  val GdgVar = "GCSGDGURI"
  val LreclMeta = "x-goog-meta-lrecl"
  private var baseDsnUri: Option[URI] = sys.env.get(DsnVar).map(new URI(_))
  private var baseGdgUri: Option[URI] = sys.env.get(GdgVar).map(new URI(_))
  def readEnv(env: Map[String,String]): Unit = {
    env.get(DsnVar).foreach {uri =>
      setBaseDsnUri(uri)
      logger.info(s"read $DsnVar from ENV: $uri")
    }
    env.get(GdgVar).foreach {uri =>
      setBaseGdgUri(uri)
      logger.info(s"read $GdgVar from ENV: $uri")
    }
  }
  def setBaseDsnUri(uri: String): Unit = baseDsnUri = Option(new URI(uri))
  def setBaseGdgUri(uri: String): Unit = baseGdgUri = Option(new URI(uri))
  def getBaseDsnUri: Option[URI] = baseDsnUri
  def getBaseGdgUri: Option[URI] = baseGdgUri

  def getUri(ds: DataSetInfo): Option[String] = {
    if (ds.gdg && CloudDataSet.getBaseGdgUri.isDefined) {
      val baseUri = CloudDataSet.getBaseGdgUri.get
      val name = CloudDataSet.buildObjectName(baseUri, ds)
      Option(s"gs://${baseUri.getAuthority}/$name")
    } else if (CloudDataSet.getBaseDsnUri.isDefined) {
      val baseUri = CloudDataSet.getBaseDsnUri.get
      val name = CloudDataSet.buildObjectName(baseUri, ds)
      Option(s"gs://${baseUri.getAuthority}/$name")
    } else None
  }

  def readCloudDDDSN(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    getBaseDsnUri match {
      case Some(baseUri) =>
        logger.info(s"Cloud DD enabled uri=$baseUri")
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
    val maybeMetadata = Option(blob.getMetadata)
    if (!maybeMetadata.exists(m => m.containsKey(LreclMeta))) {
      import scala.jdk.CollectionConverters.MapHasAsScala
      val meta = maybeMetadata.map(_.asScala.map{x => s"${x._1}=${x._2}"}.mkString("\n"))
      val msg = s"$LreclMeta not set for $uri\n$meta"
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    val lreclStr = blob.getMetadata.get(LreclMeta)
    try {
      Integer.valueOf(lreclStr)
    } catch {
      case e: NumberFormatException =>
        val msg = s"invalid lrecl '$lreclStr' for $uri"
        logger.error(msg, e)
        throw new RuntimeException(msg)
    }
  }

  def buildObjectName(baseUri: URI, ds: DataSetInfo): (String, String) =
    buildObjectName(baseUri, ds.objectName)

  def buildObjectName(base: URI, name: String): (String, String) = {
    val prefix: String = {
      val pre1 = base.getPath.stripPrefix("/").stripSuffix("/")
      if (pre1.nonEmpty) pre1 + "/"
      else ""
    }
    (prefix, name)
  }

  // prefix      gs://bucket/prefix
  // object name             prefix/HLQ.DATASET
  def readCloudDDDSN(gcs: Storage,
                     dd: String,
                     ds: DataSetInfo,
                     baseUri: URI): Option[CloudRecordReader] = {
    val bucket = baseUri.getAuthority
    val (prefix, name) = buildObjectName(baseUri, ds)
    val fullName = prefix + name
    logger.info(s"Searching for object with name=$fullName in bucket=$bucket")
    // check if DSN exists in GCS
    val blob: Blob = gcs.get(BlobId.of(bucket, fullName))
    if (blob != null) {
      val uri = toUri(blob)
      val lrecl: Int = getLrecl(blob)
      logger.info(s"Located Dataset for DD:$dd\n"+
        s"DSN=${ds.dsn}\nLRECL=$lrecl\nuri=$uri")
      Option(CloudRecordReader(ds.dsn, lrecl, bucket = bucket, prefix = prefix, name = name))
    }
    else {
      logger.info(s"GCS object doesn't exist:\ngs://$bucket/$fullName")
      None
    }
  }

  def readCloudDDGDG(gcs: Storage, dd: String, ddInfo: DataSetInfo): Option[CloudRecordReader] = {
    getBaseGdgUri match {
      case Some(baseUri) =>
        logger.info(s"Generational Cloud DD enabled uri=$baseUri")
        readCloudDDGDG(gcs, dd, ddInfo, baseUri)
      case None =>
        None
    }
  }

  def readCloudDDGDG(gcs: Storage,
                     dd: String,
                     ds: DataSetInfo,
                     baseUri: URI): Option[CloudRecordReader] = {
    val bucket = baseUri.getAuthority
    val (prefix, name) = buildObjectName(baseUri, ds)
    val fullName = prefix + name
    val uri = s"gs://$bucket/$fullName"
    logger.debug(s"Searching for object with name=$fullName, in bucket=$bucket, gdg=${ds.gdg}")
    // check if DSN exists in GCS
    import scala.jdk.CollectionConverters.IterableHasAsScala
    implicit val versions: IndexedSeq[Blob] = gcs.list(bucket,
      Storage.BlobListOption.prefix(fullName),
      Storage.BlobListOption.versions(true))
      .iterateAll.asScala.toIndexedSeq.sortBy(_.getName)

    //with Mainframe GDG version in the suffix and with latest GCS generation
    val relativeVersions = findRelativeGDGVersions(ds, name).toIndexedSeq
    logger.info(s"""
                   |GDG versions found for $uri
                   |GDG = ${ds.gdg},
                   |base GDG = ${ds.isBaseGDG},
                   |selected GDG = ${ds.isSelectedGDG},
                   |generation = ${ds.generation},
                   |found total versions = ${versions.size},
                   |found relative versions = ${relativeVersions.size},
                   |total versions = ${versions.map(b => s"name=${b.getName};generation=${b.getGeneration}").mkString("\n")}
        """.stripMargin)

    if (relativeVersions.nonEmpty) {
      val sb = new StringBuilder
      sb.append(s"Total relative versions found: ${versions.length} for $uri\n")
      sb.append("generation\tlrecl\tsize\tcreated\n")
      relativeVersions.foreach{b =>
        sb.append(b.getGeneration)
        sb.append(" ")
        sb.append(Option(b.getMetadata).getOrElse(LreclMeta,"?"))
        sb.append(" ")
        sb.append(b.getSize)
        sb.append(" ")
        sb.append(StatsUtil.epochMillis2Timestamp(b.getCreateTime))
        sb.append("\n")
      }
      logger.info(sb.result)

      val lrecls = relativeVersions.map(getLrecl)
      if (lrecls.distinct.length > 1){
        val msg = s"lrecl inconsistent across generations for $uri - found " +
          s"lrecls ${lrecls.mkString(",")}"
        logger.error(msg)
        throw new RuntimeException(msg)
      }
      val lrecl = lrecls.head
      logger.info(s"Located Generational Dataset for DD:$dd " +
        s"DSN=${ds.dsn}\nLRECL=$lrecl\nuri=$uri")
      Option(CloudRecordReader(ds.dsn, lrecl, bucket = bucket, prefix = prefix, name = name,
        gdg = true, versions = relativeVersions))
    }
    else None
  }

  def findRelativeGDGVersions(ds: DataSetInfo, objectName: String)(implicit versions: IndexedSeq[Blob]): Iterable[Blob] = {
    ds.generation match {
      case Some(v) if(ds.elementName.nonEmpty) => selectedGDG(versions, v)
      case _ => baseGDG(versions, objectName)
    }
  }

  def baseGDG(versions: IndexedSeq[Blob], baseName: String): Iterable[Blob] =
    versions.groupBy(b => b.getName).collect{case p if p._1 == baseName || isGdg(p._1) => p._2.maxBy(_.getGeneration)}

  def selectedGDG(versions: IndexedSeq[Blob], version: String): Iterable[Blob] =
    versions.sortBy(_.getGeneration).find(b => version == extractGDGVersion(b.getName).orNull).toSeq

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

    (cloudReader, cloudReaderGdg) match {
      case (Some(r), Some(r1)) =>
        // throw exception if data set exists in both buckets
        val msg =
          s"""Ambiguous Data Set Reference
             |DD:$dd
             |DSN=${ddInfo.dsn} found in both standard and generational buckets
             |Standard: ${r.uri}
             |GDG: ${r1.uri}
             |""".stripMargin
        logger.error(msg)
        throw new RuntimeException(msg)
      case x =>
        val res = x._1.orElse(x._2)
        if (res.isEmpty)
          logger.info(s"unable to find ${ddInfo.dsn} in Cloud Storage")
        res
    }
  }
}
