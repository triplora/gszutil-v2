package com.google.cloud.bqsh.cmd

import java.io.{BufferedOutputStream, OutputStreamWriter}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

import com.google.api.services.storage.StorageScopes
import com.google.cloud.bqsh.{ArgParser, Command, SdsfUtilConfig, SdsfUtilOptionParser}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.{Logging, Services}
import com.google.cloud.storage.{BlobInfo, Storage}
import com.ibm.zos.sdsf.core._

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SdsfUtil extends Command[SdsfUtilConfig] with Logging {
  override val name: String = "sdsfutil"
  override val parser: ArgParser[SdsfUtilConfig] = SdsfUtilOptionParser
  override def run(config: SdsfUtilConfig, zos: MVS): Result = {
    val runner = getJobs(config.jobPrefix, config.owner).runner
    val gcs = Services.storage(zos.getCredentialProvider().getCredentials.createScoped(StorageScopes.DEVSTORAGE_READ_WRITE))
    try {
      for (job <- runner.jobs()) {
        System.out.println("job:\n" + job)
        for (dataset <- job.dataSets) {
          System.out.println("dataset:\n" + dataset)
          save(gcs, config, job, dataset)
        }
      }
      Result.Success
    } catch {
      case e: ISFException =>
        runner.messages.foreach{x =>
          System.err.println("SDSF Messages\n" + x)
        }
        e.printStackTrace(System.err)
        Result.Failure(e.getMessage)
    }
  }

  /** Generates object name with format
    * PREFIX/JOBNAME/JOBID/STEPNAME_DDNAME
    */
  def objName(prefix: String, job: EnhancedStatus, dataset: EnhancedDataSet): String = {
    s"$prefix/${job.jobName}/${job.jobId}/${dataset.stepName}_${dataset.ddName}"
  }

  /** Write dataset lines to Cloud Storage */
  def save(gcs: Storage,
           config: SdsfUtilConfig,
           job: EnhancedStatus,
           dataset: EnhancedDataSet): Unit = {
    val name = objName(config.objPrefix, job, dataset)
    System.out.println(s"writing to gs://${config.bucket}/$name")
    val blob = gcs.create(BlobInfo.newBuilder(config.bucket,name)
      .setContentType("text/plain").build())

    val writer = new OutputStreamWriter(new BufferedOutputStream(
      Channels.newOutputStream(blob.writer()), 256*1024), StandardCharsets.UTF_8)
    for (line <- dataset.lines) {
      writer.write(line)
      writer.write("\n")
    }
    writer.close()
  }

  class EnhancedRunner(runner: ISFStatusRunner) {
    def jobs(): List[EnhancedStatus] =
      runner.exec.asScala.toList.map{x => new EnhancedStatus(x)}

    def messages: Option[String] = {
      val msgs = runner.getRequestResults.getMessageList.asScala
      if (msgs.nonEmpty) Option(msgs.mkString("\n"))
      else None
    }

    override def toString: String = {
      "ISFStatusRunner(\n" + runner.toVerboseString + "\n)"
    }
  }

  class EnhancedDataSet(dataSet: ISFJobDataSet) {
    def ddName: String = dataSet.getDDName
    def dsName: String = dataSet.getDSName
    def stepName: String = dataSet.getValue("stepn")
    def procStep: String = dataSet.getValue("procs")

    override def toString: String = {
      "ISFJobDataSet(\n" + dataSet.toVerboseString + "\n)"
    }

    def lines: List[String] = {
      val buf = ListBuffer.empty[String]
      dataSet.browse
      val settings = dataSet.getRunner.getRequestSettings
      val results = dataSet.getRunner.getRequestResults
      val lineResults = results.getLineResults
      buf.appendAll(results.getResponseList.asScala)

      settings.addISFScrollType(ISFScrollConstants.Options.DOWN)

      var token: String = lineResults.getNextLineToken
      while (token != null) {
        settings.addISFStartLineToken(token)
        dataSet.browse
        buf.appendAll(results.getResponseList.asScala)
        token = lineResults.getNextLineToken
      }
      settings.removeISFStartLineToken()
      buf.result()
    }
  }

  class EnhancedStatus(job: ISFStatus) {
    def jobName: String = job.getJName
    def jobId: String = job.getJobID
    def dataSets: List[EnhancedDataSet] = job.getJobDataSets.asScala.toList
      .map{x => new EnhancedDataSet(x)}

    override def toString: String = {
      "ISFStatus(\n" + job.toVerboseString + "\n)"
    }
  }

  class EnhancedSettings(settings: ISFRequestSettings) {
    def pre(prefix: String): EnhancedSettings = {
      settings.addISFPrefix(prefix)
      this
    }

    def own(owner: String): EnhancedSettings = {
      settings.addISFOwner(owner)
      this
    }

    def noModify: EnhancedSettings = {
      settings.addNoModify()
      this
    }

    def cols(cols: String): EnhancedSettings = {
      settings.addISFCols(cols)
      this
    }

    def sort(sortExpr: String): EnhancedSettings = {
      settings.addISFSort(sortExpr)
      this
    }

    def runner: EnhancedRunner = new EnhancedRunner(new ISFStatusRunner(settings))

    override def toString: String = {
      "ISFRequestSettings(\n" + settings.toSettingsString + "\n)"
    }
  }

  def getJobs(pre: String, owner: String): EnhancedSettings = {
    new EnhancedSettings(new ISFRequestSettings)
      .pre(pre)
      .own(owner)
      .cols("jname jobid ownerid jprio queue")
      .sort("jname a jobid a")
      .noModify
  }
}