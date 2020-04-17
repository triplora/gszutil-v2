package com.google.cloud.bqsh

import com.google.api.client.googleapis.util.Utils
import com.google.api.client.json.JsonFactory
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.{AttachedDisk, AttachedDiskInitializeParams, Instance, Metadata, NetworkInterface, Operation, ServiceAccount}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.Logging
import com.google.common.collect.ImmutableList

import scala.annotation.tailrec

object GCE extends Logging {
  private var client: Compute = _
  private def JSON: JsonFactory = Utils.getDefaultJsonFactory

  def defaultClient(credentials: Credentials): Compute = {
    if (client == null)
      client = new Compute.Builder(
        Utils.getDefaultTransport,
        Utils.getDefaultJsonFactory,
        new HttpCredentialsAdapter(credentials))
        .setApplicationName(Bqsh.UserAgent)
        .build()

    client
  }

  val Debian10 = "projects/debian-cloud/global/images/family/debian-10"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  class EnhancedInstance(val instance: Instance,
                         private val project: String,
                         private val gce: Compute) {
    def ipAddr: String = instance.getNetworkInterfaces.get(0).getNetworkIP
    def terminate(): Unit = {
      GCE.terminateVM(name = instance.getName,
        project = project,
        zone = instance.getZone,
        gce = gce)
    }

    def createInstance(): EnhancedInstance = {
      checkResponse(gce.instances.insert(project, instance.getZone, instance).execute)
      waitForInstance(gce, instance.getName, project, instance.getZone)
    }

    override def toString: String = JSON.toPrettyString(instance)
  }

  def meta(k: String, v: String): Metadata.Items =
    new Metadata.Items().setKey(k).setValue(v)

  def toMetadata(m: Map[String,String]): Metadata = {
    import scala.jdk.CollectionConverters.SeqHasAsJava
    new Metadata().setItems(m.toSeq.map{ x => meta(x._1,x._2)}.asJava)
  }

  def checkResponse(res: Operation): Unit = {
    logger.debug(res)
    val status = res.getStatus
    logger.debug(s"$status - ${res.getStatusMessage}")
    if (!("RUNNING" == status  || "DONE" == status)) {
      val msg = s"Failed to create gReceiver Compute Instance\n${JSON.toPrettyString(res)}"
      throw new RuntimeException(msg)
    }
  }

  /** Creates a gReceiver Compute Instance
    *
    * @param name name of the instance
    * @param metadata Map containing key/value pairs of metadata
    * @param serviceAccount Service Account email (sv@project.iam.gserviceaccount.com)
    * @param project Project ID
    * @param zone Zone
    * @param subnet Subnet
    * @param gce Compute client
    * @param machineType Instance type (default: n1-standard-4)
    * @return InstanceResult containing IP address and Instance object
    */
  def createVM(name: String,
               description: String,
               metadata: Map[String,String],
               serviceAccount: String,
               project: String,
               zone: String,
               subnet: String,
               gce: Compute,
               machineType: String = "n1-standard-4"): EnhancedInstance = {
    val serviceAccounts = ImmutableList.of(new ServiceAccount()
        .setEmail(serviceAccount)
        .setScopes(ImmutableList.of(StorageScope)))

    val networkInterfaces = ImmutableList.of(new NetworkInterface().setSubnetwork(subnet))

    val disks = ImmutableList.of(new AttachedDisk()
      .setType("PERSISTENT")
      .setBoot(true)
      .setMode("READ_WRITE")
      .setAutoDelete(true)
      .setDeviceName(name)
      .setInitializeParams(new AttachedDiskInitializeParams()
        .setSourceImage(Debian10)
        .setDiskType(s"projects/$project/zones/$zone/diskTypes/pd-standard")
        .setDiskSizeGb(32L)
      ))

    val instance: Instance = new Instance()
      .setDescription(description)
      .setZone(zone)
      .setName(name)
      .setCanIpForward(false)
      .setMetadata(toMetadata(metadata))
      .setMachineType(s"projects/$project/zones/$zone/machineTypes/$machineType")
      .setNetworkInterfaces(networkInterfaces)
      .setServiceAccounts(serviceAccounts)
      .setDisks(disks)

    logger.info("Requesting creation of instance")
    val eInstance = new EnhancedInstance(instance, project, gce)
    logger.debug(eInstance)

    // Create the Instance and wait for it to be running
    eInstance.createInstance()
  }

  val BadStatus = Set("STOPPING","STOPPED","SUSPENDING","SUSPENDED","TERMINATED")

  @tailrec
  def waitForInstance(gce: Compute,
                      instanceName: String,
                      project: String,
                      zone: String,
                      timeout: Long = 300000,
                      wait: Long = 5000): EnhancedInstance = {
    if (timeout <= 0)
      throw new RuntimeException("timed out waiting for instance creation")

    Option(gce.instances.get(project, zone, instanceName).execute) match {
      case Some(value) if BadStatus.contains(value.getStatus) =>
        throw new RuntimeException(s"$instanceName has status ${value.getStatus}")
      case Some(value) if "RUNNING" == value.getStatus =>
        new EnhancedInstance(value, project, gce)
      case _ =>
        Util.sleepOrYield(wait)
        waitForInstance(gce = gce,
                        instanceName = instanceName,
                        project = project,
                        zone = zone,
                        timeout = timeout - wait,
                        wait = wait)
    }
  }

  def terminateVM(name: String, project: String, zone: String, gce: Compute): Unit = {
    val req = gce.instances.delete(project, zone, name)
    logger.info(s"Requesting deletion of Instance $name")
    val res = req.execute
    logger.info(s"Delete request returned status ${res.getStatus}")
  }
}
