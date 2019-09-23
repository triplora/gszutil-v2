package com.google.cloud.bqsh

import com.google.api.client.googleapis.util.Utils
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.{AccessConfig, AttachedDisk, AttachedDiskInitializeParams, Instance, Metadata, NetworkInterface, ServiceAccount}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.gszutil.Util.Logging
import com.google.common.collect.ImmutableList

object GCE extends Logging {
  def defaultClient(credentials: Credentials): Compute = {
    new Compute.Builder(
      Utils.getDefaultTransport,
      Utils.getDefaultJsonFactory,
      new HttpCredentialsAdapter(credentials))
      .setApplicationName(Bqsh.UserAgent)
      .build();
  }

  val Debian10 = "projects/debian-cloud/global/images/debian-10-buster-v20190916"

  case class InstanceResult(ip: String, instance: Instance)

  def getStartupScript(pkgUri: String): String = {
    s"""#!/bin/bash
       |gsutil '$pkgUri' pkg.tar && tar xvf pkg.tar && . run.sh""".stripMargin
  }

  /** Creates a gReceiver Compute Instance
    *
    * @param name name of the instance
    * @param pkgUri GCS URI for tar file containing run.sh (gs://bucket/prefix/pkg.tar)
    * @param serviceAccount Service Account email (sv@project.iam.gserviceaccount.com)
    * @param project Project ID
    * @param zone Zone
    * @param subnet Subnet
    * @param gce Compute client
    * @param machineType Instance type (default: n1-standard-8)
    * @return InstanceResult containing IP address and Instance object
    */
  def createVM(name: String, pkgUri: String, serviceAccount: String,
               project: String, zone: String, subnet: String,
               gce: Compute, machineType: String = "n1-standard-8"): InstanceResult = {
    val instance: Instance = new Instance()
      .setDescription("gReceiver")
      .setZone(zone)
      .setName(name)
      .setCanIpForward(false)
      .setMetadata(new Metadata()
        .setItems(ImmutableList.of(
          new Metadata.Items()
            .setKey("startup-script")
            .setValue(getStartupScript(pkgUri))
        ))
      )
      .setMachineType(s"projects/$project/zones/$zone/machineTypes/$machineType")
      .setNetworkInterfaces(ImmutableList.of(
        new NetworkInterface()
          .setSubnetwork(s"projects/$project/regions/${zone.dropRight(2)}/subnetworks/$subnet")
      ))
      .setServiceAccounts(ImmutableList.of(
        new ServiceAccount()
          .setEmail(serviceAccount)
          .setScopes(ImmutableList.of("https://www.googleapis.com/auth/devstorage.read_write"))
      ))
      .setDisks(ImmutableList.of(
        new AttachedDisk()
          .setType("PERSISTENT")
          .setBoot(true)
          .setMode("READ_WRITE")
          .setAutoDelete(true)
          .setDeviceName(name)
          .setInitializeParams(
            new AttachedDiskInitializeParams()
              .setSourceImage(Debian10)
              .setDiskType(s"projects/$project/zones/$zone/diskTypes/pd-standard")
              .setDiskSizeGb(200L)
          )
      ))

    val instanceRequest = JacksonFactory.getDefaultInstance.toPrettyString(instance)
    logger.debug("Requesting creation of instance:")

    // Create the Instance
    val req = gce.instances().insert(project, zone, instance)
    val res = req.execute()

    // Verify Instance was created
    if (res.getStatus != "RUNNING") {
      res.setFactory(JacksonFactory.getDefaultInstance)
      val reqString = JacksonFactory.getDefaultInstance.toPrettyString(req)
      throw new RuntimeException("Failed to create gReceiver Compute " +
        "Instance:\nRequest:\n" + instanceRequest + "\nResponse:\n" + res.toPrettyString)
    }

    // Get the IP Address
    val created = gce.instances().get(project, zone, name).execute()
    val ip = created.getNetworkInterfaces.get(0).getNetworkIP

    // Return the result
    InstanceResult(ip, created)
  }
}
