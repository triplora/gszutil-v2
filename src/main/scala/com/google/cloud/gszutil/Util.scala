/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.gszutil

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, StringReader}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{MessageDigest, PrivateKey, Provider, Security}
import java.time.Instant
import java.util.zip.GZIPOutputStream
import java.util.{Collections, Date}

import com.google.api.client.auth.oauth2.{BearerToken, Credential}
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.util.Utils
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.util.{PemReader, SecurityUtils}
import com.google.auth.oauth2.{AccessToken, GSZCredentials, GoogleCredentials}
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.storage.BlobInfo
import com.google.common.base.Charsets
import com.google.common.hash.HashCode
import com.google.common.io.Resources
import com.google.protobuf.ByteString
import org.apache.log4j.{Level, Logger}
import org.zeromq.codec.Z85

import scala.util.{Failure, Random, Try}

object Util {
  def parseUri(gsUri: String): (String,String) = {
    if (gsUri.substring(0, 5) != "gs://") {
      ("", "")
    } else {
      val dest = gsUri.substring(5, gsUri.length)
      val bucket = dest.substring(0, dest.indexOf('/'))
      val path = dest.substring(dest.indexOf('/')+1, dest.length)
      (bucket, path)
    }
  }

  def printDebugInformation(): Unit = {
    import scala.collection.JavaConverters._
    val sb = new StringBuilder()
    sb.append("\n\nSystem Properties:\n")
    System.getProperties.asScala.foreach{x =>
      sb.append(x._1)
      sb.append("=")
      sb.append(x._2)
      sb.append("\n")
    }
    sb.append("\n\nEnvironment Variables:\n")
    System.getenv.asScala.foreach{x =>
      sb.append(s"${x._1}=${x._2}\n")
    }
    sb.append("\n\n")

    sb.append("JCE Providers")
    Security.getProviders.map(_.getName).foreach { x =>
      sb.append(x)
      sb.append("\n")
    }

    sb.append("\n\n")
    sb.append(showProviders())

    sb.result().lines.foreach(System.out.println)
  }

  def configureBouncyCastleProvider(): Unit = {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider())
  }

  def showProviders(): String = {
    Security.getProviders
      .map(showProvider)
      .mkString("\n\n----------------------------------------\n\n")
  }

  def showService(s: Provider.Service): String = {
    s"""Service: ${s.getAlgorithm} Type: ${s.getType} Class: ${s.getClassName}"""
  }

  def showProvider(p: Provider) : String = {
    import scala.collection.JavaConverters._
    val sb = new StringBuilder()
    sb.append("Provider: ")
    sb.append(p.getName)
    sb.append("\n")
    sb.append("Info: \n")
    sb.append(p.getInfo)
    sb.append("\n")
    sb.append("Services:\n")
    p.getServices.asScala.toArray.sortBy(_.getAlgorithm)
      .foreach{s =>
        sb.append(showService(s))
        sb.append("\n")
      }
    sb.result()
  }

  val layout = new org.apache.log4j.PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n")
  val consoleAppender = new org.apache.log4j.ConsoleAppender(layout)

  trait Logging {
    @transient
    protected lazy val logger: Logger = newLogger(this.getClass.getCanonicalName.stripSuffix("$"))
  }

  trait DebugLogging {
    @transient
    protected lazy val logger: Logger = newDebugLogger(this.getClass.getCanonicalName.stripSuffix("$"))
  }

  def newLogger(name: String, level: Level = Level.INFO): org.apache.log4j.Logger = {
      val logger = org.apache.log4j.Logger.getLogger(name)
      logger.setLevel(level)
      logger
  }

  def newDebugLogger(name: String): org.apache.log4j.Logger =
    newLogger(name, Level.DEBUG)

  def configureLogging(): Unit = {
    import org.apache.log4j.Level.{DEBUG, ERROR, WARN}
    import org.apache.log4j.Logger.{getLogger, getRootLogger}
    getRootLogger.setLevel(WARN)
    getRootLogger.addAppender(consoleAppender)
    getLogger("com.google.cloud.gszutil").setLevel(DEBUG)
    getLogger("com.google.cloud.pso").setLevel(DEBUG)
    getLogger("org.apache.orc.impl.MemoryManagerImpl").setLevel(ERROR)
  }

  val StorageScope: java.util.Collection[String] = Collections.singleton("https://www.googleapis.com/auth/devstorage.read_write")

  def readNio(path: String): Array[Byte] = {
    Files.readAllBytes(Paths.get(path))
  }

  def parseJson(json: InputStream): ServiceAccountCredential = {
    new JsonObjectParser(Utils.getDefaultJsonFactory).parseAndClose(json, StandardCharsets.UTF_8, classOf[ServiceAccountCredential])
  }

  def readCredentials(json: InputStream): GoogleCredential = {
    val parsed = parseJson(json)
    new GoogleCredential.Builder()
      .setTransport(Utils.getDefaultTransport)
      .setJsonFactory(Utils.getDefaultJsonFactory)
      .setServiceAccountId(parsed.getClientEmail)
      .setServiceAccountScopes(StorageScope)
      .setServiceAccountPrivateKey(privateKey(parsed.getPrivateKeyPem))
      .setServiceAccountPrivateKeyId(parsed.getPrivateKeyId)
      .setTokenServerEncodedUrl(parsed.getTokenUri)
      .setServiceAccountProjectId(parsed.getProjectId)
      .build()
  }

  def convertJson(json: InputStream): KeyFile = {
    val parsed = parseJson(json)
    KeyFile.newBuilder()
      .setType(parsed.getKeyType)
      .setProjectId(parsed.getProjectId)
      .setPrivateKeyId(parsed.getPrivateKeyId)
      .setPrivateKey(parsed.getPrivateKeyPem)
      .setClientEmail(parsed.getClientEmail)
      .setClientId(parsed.getClientId)
      .setAuthUri(parsed.getAuthUri)
      .setTokenUri(parsed.getTokenUri)
      .setAuthProviderX509CertUrl(parsed.getAuthProviderX509CertUrl)
      .setClientX509CertUrl(parsed.getClientX509CertUrl)
      .build()
  }

  def readPbCredentials(keyFile: KeyFile): GoogleCredential = {
    new GoogleCredential.Builder()
      .setTransport(Utils.getDefaultTransport)
      .setJsonFactory(Utils.getDefaultJsonFactory)
      .setServiceAccountId(keyFile.getClientEmail)
      .setServiceAccountScopes(StorageScope)
      .setServiceAccountPrivateKey(privateKey(keyFile.getPrivateKey))
      .setServiceAccountPrivateKeyId(keyFile.getPrivateKeyId)
      .setTokenServerEncodedUrl(keyFile.getTokenUri)
      .setServiceAccountProjectId(keyFile.getProjectId)
      .build()
  }

  def accessTokenCredentials(token: String): Credential = {
    val cred = new Credential(BearerToken.authorizationHeaderAccessMethod())
    cred.setAccessToken(token)
    cred
  }

  def accessTokenCredentials1(token: String): GoogleCredentials = {
    new GoogleCredentials(new AccessToken(token, Date.from(Instant.ofEpochMilli(System.currentTimeMillis() + 1000*60*60))))
  }

  def privateKey(privateKeyPem: String): PrivateKey = {
    SecurityUtils.getRsaKeyFactory.generatePrivate(new PKCS8EncodedKeySpec(PemReader
      .readFirstSectionAndClose(new StringReader(privateKeyPem), "PRIVATE KEY")
      .getBase64DecodedBytes))
  }

  def printException[T](x: Try[T]): Unit = {
    x match {
      case Failure(exception) =>
        System.err.println(exception.getMessage)
        exception.printStackTrace(System.err)
      case _ =>
    }
  }

  trait CredentialProvider {
    def getCredentials: GoogleCredentials
  }

  object DefaultCredentialProvider extends CredentialProvider {
    override def getCredentials: GoogleCredentials = GoogleCredentials.getApplicationDefault
  }

  case class KeyFileCredentialProvider(keyFile: KeyFile) extends CredentialProvider {
    override def getCredentials: GoogleCredentials =
      GSZCredentials.fromKeyFile(keyFile)
  }

  class ByteStringCredentialsProvider(bytes: ByteString) extends CredentialProvider {
    override def getCredentials: GoogleCredentials = GoogleCredentials.fromStream(new ByteArrayInputStream(bytes.toByteArray))
  }

  case class AccessTokenCredentialProvider(token: String) extends CredentialProvider {
    override def getCredentials: GoogleCredentials =
      accessTokenCredentials1(token)
  }

  class CountingChannel(out: WritableByteChannel) extends WritableByteChannel {
    private var open: Boolean = true
    private var bytesWritten: Long = 0
    override def write(src: ByteBuffer): Int = {
      val n = out.write(src)
      bytesWritten += n
      n
    }

    def getBytesWritten: Long = bytesWritten

    override def isOpen: Boolean = open

    override def close(): Unit = {
      open = false
      out.close()
    }
  }

  class GZIPChannel(out: WritableByteChannel, size: Int) extends WritableByteChannel {
    private val countingChannel = new CountingChannel(out)
    private val gzip = new GZIPOutputStream(Channels.newOutputStream(countingChannel), size, true)
    private val buf = new Array[Byte](size)
    private var open: Boolean = true
    def getBytesWritten: Long = countingChannel.getBytesWritten

    override def write(src: ByteBuffer): Int = {
      var n = 0
      while (src.hasRemaining){
        val m = math.min(src.remaining, buf.length)
        src.get(buf,0, m)
        gzip.write(buf, 0, m)
        n += m
      }
      n
    }

    override def isOpen: Boolean = open

    override def close(): Unit = {
      gzip.close()
      open = false
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


  case class CopyResult(hash: String, start: Long, end: Long, bytes: Long) {
    def duration: Long = end - start
    def mbps: Double = Util.mbps(bytes, duration)
    def fmbps: String = Util.fmbps(bytes, duration)
  }

  def toUri(blobInfo: BlobInfo): String =
    s"gs://${blobInfo.getBlobId.getBucket}/${blobInfo.getBlobId.getName}"

  def fmbps(bytes: Long, milliseconds: Long): String = f"${mbps(bytes,milliseconds)}%1.2f"

  def mbps(bytes: Long, milliseconds: Long): Double = ((8.0d * bytes) / (milliseconds / 1000.0d)) / 1000000.0d

  def transferStreamToChannel(in: InputStream, out: WritableByteChannel, compress: Boolean = false, chunkSize: Int = 65536): CopyResult = {
    val rc = Channels.newChannel(in)
    if (compress)
      transferWithHash(rc, new GZIPChannel(out, chunkSize), chunkSize)
    else transferWithHash(rc, out, chunkSize)
  }

  def transferWithHash(rc: ReadableByteChannel, wc: WritableByteChannel, chunkSize: Int = 4096): CopyResult = {
    val t0 = System.currentTimeMillis
    val buf = ByteBuffer.allocate(chunkSize)
    val buf2 = buf.asReadOnlyBuffer()
    val h = MessageDigest.getInstance("MD5")

    //val h = Hashing.crc32().newHasher()
    var i = 0
    var n = 0
    var totalBytesRead = 0L
    n = rc.read(buf)
    while (n > -1) {
      totalBytesRead += n
      buf.flip()
      buf2.position(buf.position)
      buf2.limit(buf.limit)
      h.update(buf2)
      wc.write(buf)
      buf.clear()
      i += 1
      n += rc.read(buf)
    }
    rc.close()
    wc.close()
    val t1 = System.currentTimeMillis
    val hash = HashCode.fromBytes(h.digest()).toString
    CopyResult(hash, t0, t1, totalBytesRead)
  }

  def readS(x: String): String = {
    new String(Resources.toByteArray(Resources.getResource(x).toURI.toURL), Charsets.UTF_8)
  }

  def readB(x: String): Array[Byte] = {
    Resources.toByteArray(Resources.getResource(x).toURI.toURL)
  }

  def readAllBytes(is: InputStream): Array[Byte] =
    readAllBytes(Channels.newChannel(is))

  def readAllBytes(in: ReadableByteChannel): Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val out = Channels.newChannel(os)
    transfer(in, out)
    os.toByteArray
  }

  def randBytes(len: Int): Array[Byte] = {
    val bytes = new Array[Byte](len)
    Random.nextBytes(bytes)
    bytes
  }

  def randString(len: Int): String =
    Z85.Z85Encoder(randBytes(len))
}
