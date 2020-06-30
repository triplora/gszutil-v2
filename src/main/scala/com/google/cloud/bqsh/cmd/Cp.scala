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

import com.google.cloud.bigquery.StatsUtil
import com.google.cloud.bqsh.{ArgParser, BQ, Command, GsUtilConfig, GsUtilOptionParser}
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.gszutil.orc.WriteORCFile
import com.google.cloud.imf.grecv.client.GRecvClient
import com.google.cloud.imf.gzos.{MVS, MVSStorage}
import com.google.cloud.imf.util.{Logging, Services}
import com.google.cloud.storage.{BlobId, Storage}


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

    val schemaProvider = c.schemaProvider.getOrElse(zos.loadCopyBook(c.copyBook))
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

    var result = Result.Failure("")
    if (c.remote){
      logger.debug(s"running with remote")
      result = GRecvClient.run(c, zos, in, schemaProvider, GRecvClient)
      logger.debug(s"remote complete")
    } else {
      logger.debug("Starting ORC Upload")
      result = WriteORCFile.run(gcsUri = c.gcsUri,
                       in = in,
                       schemaProvider = schemaProvider,
                       gcs = gcs,
                       parallelism = c.parallelism,
                       batchSize = batchSize,
                       partSizeMb = c.partSizeMB,
                       timeoutMinutes = c.timeOutMinutes,
                       compressBuffer = c.compressBuffer,
                       maxErrorPct = c.maxErrorPct)
      logger.debug("ORC Upload Complete")
    }
    in.close()
    val nRead = in.count()

    if (c.statsTable.nonEmpty){
      val statsTable = BQ.resolveTableSpec(c.statsTable, c.projectId, c.datasetId)
      logger.debug(s"writing stats to ${statsTable.getProject}:${statsTable
        .getDataset}:${statsTable.getTable}")
      val jobId = BQ.genJobId(zos,"cp")
      StatsUtil.insertJobStats(zos,jobId,job=None,
        bq = Services.bigQuery(c.projectId, c.location, creds),
        tableId = statsTable,
        jobType = "cp",
        source = sourceDSN,
        dest = c.gcsUri,
        recordsIn = nRead)
    }

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
}
