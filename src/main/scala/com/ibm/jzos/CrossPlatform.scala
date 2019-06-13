package com.ibm.jzos

import java.io.FileInputStream
import java.nio.channels.{FileChannel, ReadableByteChannel}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.Security

import com.google.cloud.gszutil.Util.{ByteStringCredentialsProvider, CredentialProvider, DefaultCredentialProvider, Logging}
import com.google.cloud.gszutil.io.{ChannelRecordReader, ZChannel, ZInputStream, ZRecordReaderT}
import com.google.cloud.gszutil.{CopyBook, Decoding}
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.google.protobuf.ByteString

import scala.util.Try

object CrossPlatform extends Logging {
  val IBM: Boolean = System.getProperty("java.vm.vendor").contains("IBM")

  def init(): Unit = {
    if (IBM) {
      Security.insertProviderAt(new com.ibm.crypto.hdwrCCA.provider.IBMJCECCA(), 1)
    }
  }

  def readChannel(dd: String, copyBook: CopyBook): ReadableByteChannel = {
    if (IBM) {
      val rr = ZOS.readDD(dd)
      require(rr.lRecl == copyBook.LRECL)
      new ZChannel(rr)
    } else {
      val ddPath = Paths.get(System.getenv(dd))
      logger.info(s"Opening $dd $ddPath")
      FileChannel.open(ddPath, StandardOpenOption.READ)
    }
  }

  def readDD(dd: String): ZRecordReaderT = {
    if (IBM) {
      ZOS.readDD(dd)
    } else {
      // On linux DD is an environment variable pointing to a file
      val env = System.getenv()
      require(env.containsKey(dd), s"$dd environment variable not set")
      val lReclKey = dd + "_LRECL"
      val blkSizeKey = dd + "_BLKSIZE"
      require(env.containsKey(lReclKey), s"$lReclKey environment variable not set")
      require(env.containsKey(blkSizeKey), s"$blkSizeKey environment variable not set")
      val lRecl: Int = env.get(dd + "_LRECL").toInt
      val blkSize: Int = env.get(dd + "_BLKSIZE").toInt
      val ddPath = Paths.get(System.getenv(dd))
      require(ddPath.toFile.exists(), s"$dd $ddPath does not exist")
      require(ddPath.toFile.isFile(), s"$dd $ddPath is not a file")
      new ChannelRecordReader(FileChannel.open(ddPath), lRecl, blkSize)
    }
  }

  def getCredentialProvider(keyFileDD: String): CredentialProvider = {
    if (IBM) {
      val bytes = ByteStreams.toByteArray(ZInputStream(keyFileDD))
      logger.debug("Loaded credential json:\n" + new String(bytes, Charsets.UTF_8))
      Try(new ByteStringCredentialsProvider(ByteString.copyFrom(bytes)))
        .getOrElse(new ByteStringCredentialsProvider(ByteString.copyFrom(Decoding.ebcdic2ascii(bytes))))
    } else {
      DefaultCredentialProvider
    }
  }

  def loadCopyBook(dd: String): CopyBook = {
    if (IBM) {
      val rr = readDD(dd)
      val copyBook = CopyBook(ByteStreams.toByteArray(ZInputStream(rr))
        .grouped(rr.lRecl)
        .map(Decoding.ebcdic2utf8)
        .mkString("\n"))
      logger.info(s"Loaded copy book with LRECL=${copyBook.LRECL} FIELDS=${copyBook.FieldNames.mkString(",")}```\n${copyBook.raw}\n```")
      copyBook
    } else {
      val ddValue = System.getenv(dd)
      require(ddValue != null, s"$dd environment variable not defined")
      val ddPath = Paths.get(ddValue)
      require(ddPath.toFile.exists(), s"$ddPath doesn't exist")
      CopyBook(new String(Files.readAllBytes(ddPath), Charsets.UTF_8))
    }
  }
}
