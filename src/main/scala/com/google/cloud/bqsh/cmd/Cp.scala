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

import com.google.cloud.bigquery.StatsUtil
import com.google.cloud.bqsh.{ArgParser,BQ,Command,GCE,GCS,GsUtilConfig,GsUtilOptionParser}
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.V2SendCallable
import com.google.cloud.gszutil.io.V2SendCallable.ReaderOpts
import com.google.cloud.gszutil.orc.WriteORCFile
import com.google.cloud.storage.Storage
import com.ibm.jzos.ZFileProvider
import org.zeromq.ZContext


object Cp extends Command[GsUtilConfig] with Logging {
  override val name: String = "gsutil cp"
  override val parser: ArgParser[GsUtilConfig] = GsUtilOptionParser
  def run(c: GsUtilConfig, zos: ZFileProvider): Result = {
    val creds = zos
      .getCredentialProvider()
      .getCredentials

    val bq = BQ.defaultClient(c.projectId, c.location, creds)
    val copyBook = zos.loadCopyBook(c.copyBook)
    val in = zos.readDDWithCopyBook(c.source, copyBook)
    logger.info(s"gsutil cp ${in.getDsn} ${c.destinationUri}")

    val batchSize = (c.blocksPerBatch * in.blkSize) / in.lRecl
    val gcs = GCS.defaultClient(creds)
    if (c.replace) {
      GsUtilRm.run(c, zos)
    } else {
      val uri = new URI(c.destinationUri)
      val withTrailingSlash = uri.getPath.stripPrefix("/") + (if (uri.getPath.last == '/') "" else "/")
      val bucket = uri.getAuthority
      val lsResult = gcs.list(bucket,
        Storage.BlobListOption.prefix(withTrailingSlash),
        Storage.BlobListOption.currentDirectory())
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter
      val blobs = lsResult.getValues.asScala.toArray
      if (blobs.nonEmpty) {
        throw new RuntimeException("Data is already present at destination. Use --replace to delete existing files prior to upload.")
      }
    }
    val sourceDSN = in.getDsn

    var result = Result.Failure("")
    if (c.remote){
      logger.info("Starting Dataset Upload")
      val instanceId = s"grecv-${zos.jobId}"
      val remoteHost = if (c.remoteHost.isEmpty) {
        logger.info(s"Creating Compute Instance $instanceId")
        Option(GCE.createVM(instanceId, c.pkgUri, c.serviceAccount,
          c.projectId, c.zone, c.subnet, GCE.defaultClient(creds), c.machineType))
      } else None
      val host = remoteHost.map(_.ip).getOrElse(c.remoteHost)
      val opts = ReaderOpts(in, copyBook, c.destinationUri, in.blkSize,
        new ZContext(), c.nConnections, host, c.remotePort)
      logger.info("Starting Send...")
      val res = V2SendCallable(opts).call()
      res.foreach(r => logger.debug(
        s"""return code: ${r.rc}
           |bytes in: ${r.bytesIn}
           |bytes out: ${r.bytesOut}
           |msgCount: ${r.msgCount}
           |yieldCount: ${r.yieldCount}""".stripMargin))
      if (res.isDefined && res.get.rc == 0) {
        logger.info("Dataset Upload Complete")
        result = Result.Success
      } else {
        logger.error("Dataset Upload Failed")
        result = Result.Failure("")
      }
      if (c.remoteHost.isEmpty) {
        if (c.projectId.nonEmpty)
          GCE.terminateVM(instanceId, c.projectId, c.zone, GCE.defaultClient(creds))
        else
          logger.warn("projectId not set")
      }
    } else {
      logger.info("Starting ORC Upload")
      result = WriteORCFile.run(gcsUri = c.destinationUri,
                       in = in,
                       copyBook = copyBook,
                       gcs = gcs,
                       maxWriters = c.parallelism,
                       batchSize = batchSize,
                       partSizeMb = c.partSizeMB,
                       timeoutMinutes = c.timeOutMinutes,
                       compress = c.compress,
                       compressBuffer = c.compressBuffer,
                       maxErrorPct = c.maxErrorPct)
      logger.info("ORC Upload Complete")
    }
    in.close()
    val nRead = in.count()

    if (c.statsTable.nonEmpty){
      logger.debug("writing stats")
      StatsUtil.insertJobStats(
        jobName=zos.jobName,
        jobDate=zos.jobDate,
        jobTime=zos.jobTime,
        job=None,
        bq=bq,
        tableId=BQ.resolveTableSpec(c.statsTable, c.projectId, c.datasetId),
        jobType="cp",
        source=sourceDSN,
        dest=c.destinationUri,
        recordsIn=nRead)
    }

    result
  }
}
