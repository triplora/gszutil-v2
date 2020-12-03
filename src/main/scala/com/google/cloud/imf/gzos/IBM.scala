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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.KeyPair

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.json.{GenericJson, JsonObjectParser}
import com.google.cloud.gszutil
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.io.{CloudRecordReader, ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.imf.gzos.JCLParser.DDStatement
import com.google.cloud.imf.gzos.MVSStorage.DSN
import com.google.cloud.imf.gzos.Util.GoogleCredentialsProvider
import com.google.cloud.imf.gzos.ZOS.{PDSIterator, RecordIterator}
import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo
import com.google.cloud.imf.util.{CloudLogging, Logging, SecurityUtils, Services}
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams


object IBM extends MVS with Logging {
  override def isIBM: Boolean = true
  override def init(): Unit = {
    ZOS.addCCAProvider()
    System.setProperty("java.net.preferIPv4Stack" , "true")
    CloudLogging.stdout("Mainframe Connector Build Info: " + Util.readS("build.txt"))
    CloudLogging.stdout(s"DDs:\n${ZOS.listDDs.mkString("\n")}")
  }

  override def exists(dsn: DSN): Boolean = ZOS.exists(dsn)
  override def ddExists(dd: String): Boolean = ZOS.ddExists(dd)
  override def getDSN(dd: String): String = ZOS.getDSN(dd)
  override def listPDS(dsn: DSN): Iterator[PDSMemberInfo] = new PDSIterator(dsn)
  override def readDSN(dsn: DSN): ZRecordReaderT = ZOS.readDSN(dsn)
  override def writeDSN(dsn: DSN): ZRecordWriterT = ZOS.writeDSN(dsn)
  override def writeDD(dd: String): ZRecordWriterT = ZOS.writeDD(dd)
  override def dsInfo(dd: String): Option[DataSetInfo] = ZOS.getDatasetInfo(dd)
  override def readDD(dd: String): ZRecordReaderT = {
    // Get DD information from z/OS
    dsInfo(dd) match {
      case Some(ddInfo) =>
        CloudLogging.stdout(s"Dataset Info for $dd:\n$ddInfo")

        // Obtain Cloud Storage client
        val gcs = Services.storage(getCredentialProvider().getCredentials)

        // check if DD exists in Cloud Storage
        CloudDataSet.readCloudDD(gcs, dd, ddInfo) match {
          case Some(r) =>
            // Prefer Cloud Data Set if DSN exists in GCS
            return r
          case _ =>
        }
      case _ =>
    }

    // Check for Local Data Set (standard z/OS DD)
    try {
      ZOS.readDD(dd)
    } catch {
      // Handle DD open failure
      case _: Throwable =>
        val gcsDD = sys.env.getOrElse("GCSDD","GCSLOC")
        try {
          // Read DDs from data set
          val gcsParm = readDDString(gcsDD,"\n")
          CloudLogging.stdout(s"GCS PARM:\n$gcsParm")
          // Parse DDs
          val gcsDDs = JCLParser.splitStatements(gcsParm)
          CloudLogging.stdout(s"GCS DSNs:\n${gcsDDs.mkString("\n")}")
          // Search for requested DDNAME
          gcsDDs.find(_.name == dd) match {
            case Some(DDStatement(_, lrecl, dsn)) =>
              CloudRecordReader(dsn, lrecl)
            case None =>
              throw new RuntimeException(s"Cloud DD '$dd' not found in $gcsDD")
          }
        } catch {
          case t: Throwable =>
            throw new RuntimeException(s"DD not found: $dd", t)
        }
    }
  }

  override def readDSNLines(dsn: DSN): Iterator[String] =
    new RecordIterator(readDSN(dsn)).takeWhile(_ != null)

  override def readStdin(): String = {
    val in = ByteStreams.toByteArray(System.in)
    new String(in, Ebcdic.charset)
  }

  override def readDDString(dd: String, recordSeparator: String): String = {
    val in = ZOS.readDD(dd)
    val bytes = Util.readAllBytes(in)
    val decoded = Ebcdic.decodeBytes(bytes)
    Util.records2string(decoded, in.lRecl, Charsets.UTF_8, recordSeparator)
  }

  private var cp: CredentialProvider = _
  private var keypair: KeyPair = _
  private var principal: String = _
  private var keyfileBytes: Array[Byte] = _

  override def getPrincipal(): String = {
    if (principal == null) getCredentialProvider()
    principal
  }

  override def getCredentialProvider(): CredentialProvider = {
    if (cp != null) cp
    else {
      if (keyfileBytes == null) keyfileBytes = Util.readAllBytes(readDD("KEYFILE"))
      val json = new JsonObjectParser(JacksonFactory.getDefaultInstance)
        .parseAndClose(new ByteArrayInputStream(keyfileBytes), StandardCharsets.UTF_8, classOf[GenericJson])
      principal = json.get("client_email").asInstanceOf[String]
      // todo read keypair
      keypair = SecurityUtils.readServiceAccountKeyPair(keyfileBytes)
      cp = new GoogleCredentialsProvider(keyfileBytes)
      cp
    }
  }

  override def getKeyPair(): KeyPair = {
    if (keypair == null) getCredentialProvider()
    keypair
  }

  override def loadCopyBook(dd: String): CopyBook = {
    val raw = readDDString(dd, "\n")
    logger.info(s"Parsing copy book:\n$raw")
    try {
      val copyBook = CopyBook(raw, Ebcdic)
      logger.info(s"Loaded copy book with LRECL=${copyBook.LRECL}")
      copyBook
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to parse copybook", e)
    }
  }

  override def jobId: String = ZOS.getJobId
  override def jobName: String = ZOS.getJobName
  override def jobDate: String = sys.env.getOrElse("JOBDATE","UNKNOWN")
  override def jobTime: String = sys.env.getOrElse("JOBTIME","UNKNOWN")
  override def getInfo: ZOSJobInfo = ZOS.getInfo
  override def getSymbol(s: String): Option[String] = ZOS.getSymbol(s)
  override def substituteSystemSymbols(s: String): String = ZOS.substituteSystemSymbols(s)
  override def submitJCL(jcl: Seq[String]): Option[ZMVSJob] = ZOS.submitJCL(jcl)
  override def transcoder: gszutil.Transcoder = Ebcdic
}
