package com.google.cloud.imf.grecv

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GCE.EnhancedInstance
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.bqsh.{GCE, GsUtilConfig}
import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.ZRecordReaderT
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.gzos.{MVS, Util}
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.protobuf.ByteString

object GRecvClient extends Logging {
  def run(cfg: GsUtilConfig,
          zos: MVS,
          creds: GoogleCredentials,
          in: ZRecordReaderT,
          schemaProvider: SchemaProvider,
          receiver: Receiver): Result = {
    logger.info("Starting Dataset Upload")
    val vm =
      if (cfg.remoteHost.nonEmpty) None
      else createVM(cfg, creds, zos.jobId.toLowerCase)

    try {
      logger.info("Starting Send...")
      val keypair = zos.getKeyPair()
      val req = GRecvRequest.newBuilder
          .setSchema(schemaProvider.toRecordBuilder.build)
          .setBasepath(cfg.gcsUri)
          .setLrecl(in.lRecl)
          .setBlksz(in.blkSize)
          .setMaxErrPct(cfg.maxErrorPct)
          .setJobinfo(zos.getInfo)
          .setPrincipal(zos.getPrincipal())
          .setPublicKey(ByteString.copyFrom(keypair.getPublic.getEncoded))
          .setTimestamp(System.currentTimeMillis())

      req.setSignature(ByteString.copyFrom(SecurityUtils.sign(keypair.getPrivate,
        req.clearSignature.build.toByteArray)))

      val h = vm.map(_.ipAddr).getOrElse(cfg.remoteHost)
      val tls = !Util.isIbm && cfg.tls
      receiver.recv(req.build, h, cfg.remotePort, cfg.nConnections, cfg.parallelism, tls, in)
    } catch {
      case e: Throwable =>
        logger.error("Dataset Upload Failed", e)
        Result.Failure(e.getMessage)
    } finally {
      vm.foreach(_.terminate())
    }
  }

  def startupScript(pkgUri: String): String =
    s"""#!/bin/bash
       |gsutil cp '$pkgUri/run.sh' .
       |. run.sh""".stripMargin

  def createVM(c: GsUtilConfig,
               creds: GoogleCredentials,
               jobId: String): Option[EnhancedInstance] = {
    if (c.remoteHost.isEmpty) {
      val instanceName = s"grecv-$jobId"
      logger.info(s"Creating Compute Instance $instanceName")
      val vmSubnet = if (c.subnet.split("/").length == 1) {
        s"projects/${c.projectId}/regions/${c.zone.dropRight(2)}/subnetworks/${c.subnet}"
      } else c.subnet
      val metadata = Map(
        "startup-script" -> startupScript(c.pkgUri),
        "pkguri" -> c.pkgUri
      )
      Option(GCE.createVM(name = instanceName,
        description = "grecv",
        metadata = metadata,
        serviceAccount = c.serviceAccount,
        project = c.projectId,
        zone = c.zone,
        subnet = vmSubnet,
        gce = GCE.defaultClient(creds),
        machineType = c.machineType))
    } else None
  }
}
