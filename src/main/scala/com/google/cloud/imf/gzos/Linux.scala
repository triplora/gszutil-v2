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

import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.{KeyPair, KeyPairGenerator}
import java.util.Date

import com.google.cloud.bigquery.StatsUtil
import com.google.cloud.gszutil
import com.google.cloud.gszutil.io.{ChannelRecordReader, ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.gszutil.{CopyBook, Utf8}
import com.google.cloud.imf.gzos.MVSStorage.DSN
import com.google.cloud.imf.gzos.Util.DefaultCredentialProvider
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.imf.util.Logging
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams

object Linux extends MVS with Logging {
  override def isIBM: Boolean = false
  override def init(): Unit = {
    System.setProperty("java.net.preferIPv4Stack" , "true")
    System.out.println("Build Info:\n" + Util.readS("build.txt"))
  }

  override def ddExists(dd: String): Boolean = {
    sys.env.contains(dd) && sys.env.contains(dd+"_LRECL") && sys.env.contains(dd+"_BLKSIZE")
  }

  override def getDSN(dd: String): String = dd

  override def readDD(dd: String): ZRecordReaderT = ddFile(dd)

  override def readStdin(): String = {
    val in = ByteStreams.toByteArray(System.in)
    new String(in, Charsets.UTF_8)
  }

  override def readDDString(dd: String, recordSeparator: String): String = {
    val in = readDD(dd)
    val bytes = Util.readAllBytes(in)
    Util.records2string(bytes, in.lRecl, Charsets.UTF_8, recordSeparator)
  }

  override def getPrincipal(): String = System.getProperty("user.name")

  override def getCredentialProvider(): CredentialProvider = new DefaultCredentialProvider

  override def getKeyPair(): KeyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair

  override def loadCopyBook(dd: String): CopyBook = {
    val ddValue = System.getenv(dd)
    require(ddValue != null, s"$dd environment variable not defined")
    val ddPath = Paths.get(ddValue)
    require(ddPath.toFile.exists(), s"$ddPath doesn't exist")
    CopyBook(new String(Files.readAllBytes(ddPath), Charsets.UTF_8), transcoder)
  }

  /** On Linux DD is an environment variable pointing to a file
    */
  protected def ddFile(dd: String): ZRecordReaderT = {
    val env = System.getenv()
    require(env.containsKey(dd), s"$dd environment variable not set")
    val ddPath = Paths.get(System.getenv(dd))
    logger.info(s"Opening $dd $ddPath")
    val lReclKey = dd + "_LRECL"
    val blkSizeKey = dd + "_BLKSIZE"
    require(env.containsKey(lReclKey), s"$lReclKey environment variable not set")
    require(env.containsKey(blkSizeKey), s"$blkSizeKey environment variable not set")
    val lRecl: Int = env.get(dd + "_LRECL").toInt
    val blkSize: Int = env.get(dd + "_BLKSIZE").toInt
    val ddFile = ddPath.toFile
    require(ddFile.exists, s"$dd $ddPath does not exist")
    require(ddFile.isFile, s"$dd $ddPath is not a file")
    new ChannelRecordReader(FileChannel.open(ddPath, StandardOpenOption.READ), lRecl, blkSize)
  }

  override def jobName: String = sys.env.getOrElse("JOBNAME","JOBNAME")

  override def jobDate: String = StatsUtil.JobDateFormat.format(new Date())

  override def jobTime: String = StatsUtil.JobTimeFormat.format(new Date())

  override def getInfo: ZOSJobInfo = ZOSJobInfo.newBuilder
    .setJobid(jobId)
    .setJobdate(jobDate)
    .setJobtime(jobTime)
    .setJobname(jobName)
    .setStepName(sys.env.getOrElse("JOB_STEP","STEP01"))
    .setProcStepName(sys.env.getOrElse("PROC_STEP","STEP01"))
    .setUser(System.getProperty("user.name"))
    .build

  override def getSymbol(s: String): Option[String] = throw new NotImplementedError()

  override def substituteSystemSymbols(s: String): String = s

  override def exists(dsn: DSN): Boolean = throw new NotImplementedError()
  override def readDSN(dsn: DSN): ZRecordReaderT = throw new NotImplementedError()
  override def readDSNLines(dsn: DSN): Iterator[String] = throw new NotImplementedError()
  override def writeDSN(dsn: DSN): ZRecordWriterT = throw new NotImplementedError()
  override def listPDS(dsn: DSN): Iterator[PDSMemberInfo] = throw new NotImplementedError()

  override def submitJCL(jcl: Seq[String]): Option[ZMVSJob] = throw new NotImplementedError()

  override def transcoder: gszutil.Transcoder = Utf8
}
