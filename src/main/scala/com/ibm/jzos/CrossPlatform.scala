package com.ibm.jzos

import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.google.cloud.gszutil.Util.{CredentialProvider, DefaultCredentialProvider, GoogleCredentialsProvider, Logging}
import com.google.cloud.gszutil.io._
import com.google.cloud.gszutil.{CopyBook, Decoding, Util}
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams

/** Provides methods that work on both Linux and z/OS
  * Does not import any IBM classes directly to enable testing
  */
object CrossPlatform extends Logging {
  val IBM: Boolean = System.getProperty("java.vm.vendor").contains("IBM")

  def init(): Unit = {
    if (IBM) {
      ZOS.addCCAProvider()
    }
  }

  /** Opens a ReadableByteChannel
    *
    * @param dd DD name of input dataset
    * @param copyBook used to verify LRECL of the dataset
    * @return
    */
  def readChannel(dd: String, copyBook: CopyBook): DDChannel = {
    if (IBM) {
      val rr = ZOS.readDD(dd)
      require(rr.lRecl == copyBook.LRECL)
      DDChannel(new ZChannel(rr), rr.lRecl, rr.blkSize)
    } else {
      val ddc = ddFile(dd)
      require(ddc.lRecl == copyBook.LRECL)
      ddc
    }
  }

  /** On Linux DD is an environment variable pointing to a file
   */
  private def ddFile(dd: String): DDChannel = {
    val ddPath = Paths.get(System.getenv(dd))
    logger.info(s"Opening $dd $ddPath")
    val lReclKey = dd + "_LRECL"
    val blkSizeKey = dd + "_BLKSIZE"
    val env = System.getenv()
    require(env.containsKey(lReclKey), s"$lReclKey environment variable not set")
    require(env.containsKey(blkSizeKey), s"$blkSizeKey environment variable not set")
    val lRecl: Int = env.get(dd + "_LRECL").toInt
    val blkSize: Int = env.get(dd + "_BLKSIZE").toInt
    val ddFile = ddPath.toFile
    require(ddFile.exists, s"$dd $ddPath does not exist")
    require(ddFile.isFile, s"$dd $ddPath is not a file")
    DDChannel(FileChannel.open(ddPath, StandardOpenOption.READ), lRecl, blkSize)
  }

  def readDD(dd: String): ZRecordReaderT = {
    if (IBM) {
      ZOS.readDD(dd)
    } else {
      val ddc = ddFile(dd)
      new ChannelRecordReader(ddc.rc, ddc.lRecl, ddc.blkSize)
    }
  }

  def readDDString(dd: String): String = {
    val in = CrossPlatform.readDD(dd)
    val bytes = Util.readAllBytes(new ZChannel(in))
    if (IBM) {
      bytes.grouped(in.lRecl)
        .map(new String(_, Decoding.CP1047).trim)
        .mkString("\n")
    } else {
      new String(bytes, Charsets.UTF_8)
    }
  }

  def getCredentialProvider(keyFileDD: String): CredentialProvider = {
    if (IBM) {
      val bytes = ByteStreams.toByteArray(ZInputStream(keyFileDD))
      new GoogleCredentialsProvider(bytes)
    } else {
      new DefaultCredentialProvider
    }
  }

  def loadCopyBook(dd: String): CopyBook = {
    if (IBM) {
      val copyBook = CopyBook(readDDString(dd))
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
