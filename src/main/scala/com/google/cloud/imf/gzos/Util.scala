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
package com.google.cloud.imf.gzos

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.charset.Charset
import java.util

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.gszutil.io.{ZDataSet, ZRecordReaderT}
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.imf.util.{Logging, StackDriverLoggingAppender}
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.storage.BlobInfo
import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSet
import com.google.common.io.{BaseEncoding, Resources}
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

import scala.util.Random

object Util extends Logging {
  final val isIbm = System.getProperty("java.vm.vendor").contains("IBM")
  def zProvider: MVS = if (isIbm) IBM else Linux
  def sleepOrYield(ms: Long): Unit = {
    if (isIbm) {
      logger.debug(s"Yielding for $ms ms...")
      val t1 = System.currentTimeMillis + ms
      while (System.currentTimeMillis < t1){
        Thread.`yield`()
      }
    } else {
      logger.debug(s"Waiting for $ms ms...")
      Thread.sleep(ms)
    }
  }

  private var sdlAppender: StackDriverLoggingAppender = _
  def setZInfo(zInfo: ZOSJobInfo): Unit = {
    if (sdlAppender != null)
      sdlAppender.setJobInfo(zInfo)
  }

  def configureLogging(): Unit = configureLogging(false, sys.env)

  def configureLogging(debugOverride: Boolean = false, env: Map[String,String] = sys.env): Unit = {
    val debug = env.getOrElse("BQSH_ROOT_LOGGER","").contains("DEBUG") || debugOverride
    val rootLogger = LogManager.getRootLogger

    if (!rootLogger.getAllAppenders.hasMoreElements) {
      rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%d{ISO8601} %-5p %c %x - %m%n")))

      (env.get("LOG_PROJECT"), env.get("LOG_NAME")) match {
        case (Some(project), Some(logName)) =>
          logger.debug("adding StackDriverLoggingAppender")
          sdlAppender = StackDriverLoggingAppender(logName,
            LoggingOptions.newBuilder().setProjectId(project).build.getService)
          rootLogger.addAppender(sdlAppender)
        case _ =>
          None
      }

      LogManager
        .getLogger("org.apache.orc.impl")
        .setLevel(Level.ERROR)
      LogManager
        .getLogger("io.grpc.netty")
        .setLevel(Level.ERROR)
      LogManager
        .getLogger("io.netty")
        .setLevel(Level.ERROR)
      LogManager
        .getLogger("org.apache.http")
        .setLevel(Level.WARN)
    }

    if (debug) {
      rootLogger.setLevel(Level.DEBUG)
    } else {
      rootLogger.setLevel(Level.INFO)
    }
  }

  private val r = Runtime.getRuntime

  def logMem(): String = {
    val free = r.freeMemory() / (1024L * 1024L)
    val total = r.totalMemory() / (1024L * 1024L)
    val used = total - free
    s"Memory: ${used}M used\t${free}M free\t${total}M total"
  }

  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val ComputeScope = "https://www.googleapis.com/auth/compute"
  final val Scopes = ImmutableSet.of(ComputeScope, StorageScope, BigQueryScope)

  class DefaultCredentialProvider extends CredentialProvider {
    private val credentials =
      GoogleCredentials
        .getApplicationDefault
        .createScoped(Scopes)
    override def getCredentials: GoogleCredentials = {
      credentials.refreshIfExpired()
      credentials
    }
  }

  class GoogleCredentialsProvider(bytes: Array[Byte]) extends CredentialProvider {
    private val credentials: GoogleCredentials =
      GoogleCredentials
        .fromStream(new ByteArrayInputStream(bytes))
        .createScoped(Scopes)
    override def getCredentials: GoogleCredentials = {
      credentials.refreshIfExpired()
      credentials
    }
  }

  def transfer(rc: ReadableByteChannel, wc: WritableByteChannel, chunkSize: Int = 4096): Unit = {
    val buf = ByteBuffer.allocate(chunkSize)
    while (rc.read(buf) > -1) {
      buf.flip()
      wc.write(buf)
      buf.clear()
    }
    rc.close()
    wc.close()
  }

  def toUri(blobInfo: BlobInfo): String =
    s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}"

  def fmbps(bytes: Long, milliseconds: Long): String = f"${mbps(bytes,milliseconds)}%1.2f"

  def mbps(bytes: Long, milliseconds: Long): Double = ((8.0d * bytes) / (milliseconds / 1000.0d)) / 1000000.0d

  def readS(x: String): String = {
    new String(Resources.toByteArray(Resources.getResource(x).toURI.toURL), Charsets.UTF_8)
  }

  def readB(x: String): Array[Byte] = {
    Resources.toByteArray(Resources.getResource(x).toURI.toURL)
  }

  def readAllBytes(in: ReadableByteChannel): Array[Byte] = {
    val chunkSize = in match {
      case x: ZRecordReaderT =>
        x.blkSize
      case _ =>
        4096
    }
    val os = new ByteArrayOutputStream()
    val out = Channels.newChannel(os)
    transfer(in, out, chunkSize)
    os.toByteArray
  }

  def randBytes(len: Int): Array[Byte] = {
    val bytes = new Array[Byte](len)
    Random.nextBytes(bytes)
    bytes
  }

  def random(len: Int, lrecl: Int, blksz: Int): ZDataSet = {
    new ZDataSet(randBytes(len), lrecl, blksz)
  }

  def randString(len: Int): String =
    BaseEncoding.base64Url().encode(randBytes(len)).substring(0,len)

  def trimRight(s: String, c: Char): String = {
    var i = s.length
    while (i > 0 && s.charAt(i-1) == c) {
      i -= 1
    }
    if (i < s.length)
      s.substring(0,i)
    else s
  }

  def records2string(bytes: Array[Byte], lRecl: Int, charset: Charset, recordSeparator: String): String = {
    bytes.grouped(lRecl)
      .map{b => trimRight(new String(b, charset),' ')}
      .mkString(recordSeparator)
  }

  def collectJobInfo(zos: MVS): util.Map[String,String] = {
    val info = zos.getInfo
    System.out.println("JES Symbols:")
    for ((k,v) <- info.symbols)
      System.out.println(s"  $k=$v")
    val script = zos.readStdin()
    val content = new util.HashMap[String,String]()
    content.put("jobid", zos.jobId)
    content.put("jobdate", zos.jobDate)
    content.put("jobtime", zos.jobTime)
    content.put("jobname", zos.jobName)
    content.put("stepname", info.stepName)
    content.put("procstepname", info.procStepName)
    content.put("symbols", info.symbols.map(x => s"${x._1}=${x._2}").mkString("\n"))
    content.put("user", info.user)
    content.put("script", script)
    content
  }
}
