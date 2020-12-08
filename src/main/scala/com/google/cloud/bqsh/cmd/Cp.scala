/*
 * Copyright 2019 Google LLC All Rights Reserved.
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
package com.google.cloud.bqsh.cmd

import java.net.URI
import java.nio.channels.Channels
import java.nio.file.Paths

import com.google.cloud.bqsh.{ArgParser, BQ, Command, GsUtilConfig, GsUtilOptionParser}
import com.google.cloud.gszutil.Decoding.{CopyBookField, CopyBookLine, CopyBookTitle}
import com.google.cloud.gszutil.{CopyBook, Decoder, Decoding, RecordSchema, SchemaProvider}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.gszutil.orc.WriteORCFile
import com.google.cloud.imf.grecv.client.GRecvClient
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.{MVS, MVSStorage}
import com.google.cloud.imf.util.{Logging, Services, StatsUtil}
import com.google.cloud.storage.{BlobId, BucketInfo, Storage}
import com.google.protobuf.util.JsonFormat


object Cp extends Command[GsUtilConfig] with Logging {
  override val name: String = "gsutil cp"
  override val parser: ArgParser[GsUtilConfig] = GsUtilOptionParser
  def run(c: GsUtilConfig, zos: MVS): Result = {
    val creds = zos
      .getCredentialProvider()
      .getCredentials
    val gcs = Services.storage(creds)

    if (c.destPath.nonEmpty) {
      return cpFs(c.gcsUri, c.destPath, gcs, zos)
    } else if (c.destDSN.nonEmpty) {
      return cpDsn(c.gcsUri, c.destDSN, gcs, zos)
    }


    val schemaProvider: SchemaProvider = parseRecord(getTransformationsAsString(c, gcs, zos)) match {
      case Some(x) => merge(c.schemaProvider.getOrElse(zos.loadCopyBook(c.copyBook)), x)
      case None => c.schemaProvider.getOrElse(zos.loadCopyBook(c.copyBook))
    }

    val in: ZRecordReaderT = c.testInput.getOrElse(zos.readDD(c.source))
    logger.info(s"gsutil cp ${in.getDsn} ${c.gcsUri}")
    val batchSize = (c.blocksPerBatch * in.blkSize) / in.lRecl
    if (c.replace) {
      GsUtilRm.run(c.copy(recursive = true), zos)
    } else {
      val uri = new URI(c.gcsUri)
      val withTrailingSlash = uri.getPath.stripPrefix("/").stripSuffix("/") + "/"
      val bucket = uri.getAuthority
      val lsResult = gcs.list(bucket,
        Storage.BlobListOption.prefix(withTrailingSlash),
        Storage.BlobListOption.currentDirectory())
      import scala.jdk.CollectionConverters.IterableHasAsScala
      if (lsResult.getValues.asScala.nonEmpty) {
        val msg = s"Data exists at $uri;" +
          "use --replace to remove existing files prior to upload."
        logger.error(msg)
        return Result.Failure(msg)
      }
    }
    val sourceDSN = in.getDsn

    val result =
      if (c.remote) {
        val c1 =
          if (c.gcsDSNPrefix.isEmpty)
            c.copy(gcsDSNPrefix = sys.env.getOrElse("GCSDSNPREFIX", sys.env("GCSPREFIX")))
          else c
        GRecvClient.run(c1, zos, in, schemaProvider, GRecvClient)
      }
      else WriteORCFile.run(
        gcsUri = c.gcsUri,
        in = in,
        schemaProvider = schemaProvider,
        gcs = gcs,
        parallelism = c.parallelism,
        batchSize = batchSize,
        zos,
        maxErrorPct = c.maxErrorPct)
    in.close()
    val nRead = in.count()

    if (c.statsTable.nonEmpty){
      val statsTable = BQ.resolveTableSpec(c.statsTable, c.projectId, c.datasetId)
      logger.debug(s"writing stats to ${statsTable.getProject}:${statsTable
        .getDataset}:${statsTable.getTable}")
      val jobId = BQ.genJobId(c.projectId, c.location, zos,"cp")
      StatsUtil.insertJobStats(
        zos = zos,
        jobId = jobId,
        job = None,
        bq = Services.bigQuery(c.projectId, c.location, creds),
        tableId = statsTable,
        jobType = "cp",
        source = sourceDSN,
        dest = c.gcsUri,
        recordsIn = nRead,
        recordsOut = result.activityCount)
    }

    // cleanup temp files
    GsUtilRm.run(c.copy(recursive = true,
      gcsUri = c.gcsUri.stripSuffix("/") + "/tmp"), zos)

    result
  }

  def cpFs(srcUri: String, destPath: String, gcs: Storage, zos: MVS): Result = {
    val uri = new URI(srcUri)
    Option(gcs.get(BlobId.of(uri.getAuthority, uri.getPath))) match {
      case Some(value) =>
        val dest =
          if (!srcUri.endsWith("/")) Paths.get(destPath)
          else Paths.get(destPath + srcUri.reverse.dropWhile(_ != '/').reverse)

        logger.debug(s"gsutil cp $uri $dest")
        value.downloadTo(dest)
        Result.Success
      case None =>
        Result.Failure(s"$srcUri doesn't exist")
    }
  }

  def cpDsn(srcUri: String, destDSN: String, gcs: Storage, zos: MVS): Result = {
    val uri = new URI(srcUri)
    Option(gcs.get(BlobId.of(uri.getAuthority, uri.getPath))) match {
      case Some(value) =>
        val w = zos.writeDSN(MVSStorage.parseDSN(destDSN))

        logger.debug(s"gsutil cp $uri $destDSN")
        value.downloadTo(Channels.newOutputStream(w))
        Result.Success
      case None =>
        Result.Failure(s"$srcUri doesn't exist")
    }
  }


  def parseRecord(json: Option[String]): Option[Record] = {
    json match {
      case Some(x) => {
        val builder = Record.newBuilder
        JsonFormat.parser.ignoringUnknownFields.merge(x, builder)
        Option(builder.build)
      }
      case _ => None
    }
  }

  def getTransformationsAsString(c: GsUtilConfig, gcs: Storage, zos: MVS): Option[String] = {
    if (c.tfGCS.nonEmpty) {
      BucketInfo.of(c.tfGCS).getName
      logger.info(s"Reading transformation from GCS: ${c.tfGCS} ")
      val objName = c.tfGCS.split("\\")(c.tfGCS.split("\\").length - 1)
      logger.info(s"Object name: $objName ")
      Option(gcs.get(BlobId.of(BucketInfo.of(c.tfGCS).getName, objName))) match {
        case Some(value) => Option(new String(value.getContent()))
        case _ => None
      }
    }
    else
      Option(zos.readDDString("TRANF", ""))
  }

  def merge(s: SchemaProvider, r: Record): SchemaProvider = {
    s match {
      case x: CopyBook =>

        import scala.jdk.CollectionConverters.IterableHasAsScala

        val seq1: List[Record.Field] = r.getFieldList.asScala.toList
        val names= seq1.map(_.getName)
        val v: Seq[CopyBookLine] = x.Fields.filterNot({
          case CopyBookField(name, _) =>
            names.contains(name)
          case CopyBookTitle(_) => false
        })
        CopyBook(x.raw, x.transcoder, Option(v ++ seq1.map(
          fld =>
            CopyBookField(fld.getName, Decoding.getDecoder(fld, x.transcoder))
        )))
      case y: RecordSchema => y
    }
  }
}
