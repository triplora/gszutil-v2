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
import com.google.cloud.gszutil.io.{ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.imf.gzos.MVSStorage.DSN
import com.google.cloud.imf.gzos.Util.GoogleCredentialsProvider
import com.google.cloud.imf.gzos.ZOS.{PDSIterator, RecordIterator}
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams

object IBM extends MVS with Logging {
  override def isIBM: Boolean = true
  override def init(): Unit = {
    ZOS.addCCAProvider()
    System.setProperty("java.net.preferIPv4Stack" , "true")
    System.out.println("Build Info:\n" + Util.readS("build.txt"))
  }

  override def exists(dsn: DSN): Boolean = ZOS.exists(dsn)
  override def ddExists(dd: String): Boolean = ZOS.ddExists(dd)
  override def getDSN(dd: String): String = ZOS.getDSN(dd)
  override def listPDS(dsn: DSN): Iterator[PDSMemberInfo] = new PDSIterator(dsn)
  override def readDSN(dsn: DSN): ZRecordReaderT = ZOS.readDSN(dsn)
  override def writeDSN(dsn: DSN): ZRecordWriterT = ZOS.writeDSN(dsn)
  override def readDD(dd: String): ZRecordReaderT = ZOS.readDD(dd)

  override def readDSNLines(dsn: DSN): Iterator[String] =
    new RecordIterator(readDSN(dsn)).takeWhile(_ != null)

  override def readStdin(): String = {
    val in = ByteStreams.toByteArray(System.in)
    new String(in, Ebcdic.charset)
  }

  override def readDDString(dd: String, recordSeparator: String): String = {
    val in = readDD(dd)
    val bytes = Util.readAllBytes(in)
    val decoded = Ebcdic.decodeBytes(bytes)
    Util.records2string(decoded, in.lRecl, Charsets.UTF_8, recordSeparator)
  }

  private var cp: CredentialProvider = _
  private var keypair: KeyPair = _
  private var principal: String = _

  override def getPrincipal(): String = {
    if (principal == null) getCredentialProvider()
    principal
  }

  override def getCredentialProvider(): CredentialProvider = {
    if (cp != null) cp
    else {
      val bytes = Util.readAllBytes(readDD("KEYFILE"))
      val json = new JsonObjectParser(JacksonFactory.getDefaultInstance)
        .parseAndClose(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8, classOf[GenericJson])
      principal = json.get("client_email").asInstanceOf[String]
      // todo read keypair
      keypair = SecurityUtils.readServiceAccountKeyPair(bytes)
      cp = new GoogleCredentialsProvider(bytes)
      cp
    }
  }

  override def getKeyPair(): KeyPair = {
    if (keypair == null) getCredentialProvider()
    keypair
  }

  override def loadCopyBook(dd: String): CopyBook = {
    val raw = readDDString(dd, "\n")
    logger.debug(s"Parsing copy book:\n$raw")
    try {
      val copyBook = CopyBook(raw, Ebcdic)
      logger.info(s"Loaded copy book:\n$copyBook")
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
  override def getInfo: ZInfo = ZOS.getInfo
  override def getSymbol(s: String): Option[String] = ZOS.getSymbol(s)
  override def substituteSystemSymbols(s: String): String = ZOS.substituteSystemSymbols(s)
  override def submitJCL(jcl: Seq[String]): Option[ZMVSJob] = ZOS.submitJCL(jcl)
  override def transcoder: gszutil.Transcoder = Ebcdic
}