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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.file.{Files, Paths}
import java.security.{MessageDigest, Provider, Security}
import java.util.Collections
import java.util.zip.GZIPOutputStream

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.BlobInfo
import com.google.common.base.Charsets
import com.google.common.hash.HashCode
import com.google.common.io.Resources
import com.google.protobuf.ByteString
import org.apache.log4j.{ConsoleAppender, Level, LogManager, Logger, PatternLayout}
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

  val layout = new PatternLayout("%d{ISO8601} %-5p %c %x - %m%n")
  val consoleAppender = new ConsoleAppender(layout)

  trait Logging {
    @transient
    protected lazy val logger: Logger = LogManager.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))
  }

  def configureLogging(debug: Boolean = false): Unit = {
    val rootLogger = LogManager.getRootLogger
    rootLogger.addAppender(consoleAppender)
    LogManager.getLogger("org.apache.orc.impl.MemoryManagerImpl").setLevel(Level.ERROR)

    if (debug) {
      rootLogger.setLevel(Level.DEBUG)
    } else {
      rootLogger.setLevel(Level.WARN)
      LogManager.getLogger("com.google.cloud.gszutil").setLevel(Level.INFO)
      LogManager.getLogger("com.google.cloud.pso").setLevel(Level.INFO)
    }
  }

  private val r = Runtime.getRuntime

  def logMem(): String = {
    val free = r.freeMemory() / (1024L * 1024L)
    val total = r.totalMemory() / (1024L * 1024L)
    val used = total - free
    s"Memory: ${used}M used\t${free}M free\t${total}M total"
  }

  val StorageScope: java.util.Collection[String] = Collections.singleton("https://www.googleapis.com/auth/devstorage.read_write")

  def readNio(path: String): Array[Byte] = {
    Files.readAllBytes(Paths.get(path))
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

  class ByteStringCredentialsProvider(bytes: ByteString) extends CredentialProvider {
    override def getCredentials: GoogleCredentials = GoogleCredentials.fromStream(new ByteArrayInputStream(bytes.toByteArray))
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
