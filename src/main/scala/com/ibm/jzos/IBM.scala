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

package com.ibm.jzos

import com.google.cloud.gszutil.Util.{CredentialProvider, GoogleCredentialsProvider, Logging,
  PDSMemberInfo, ZInfo, ZMVSJob}
import com.google.cloud.gszutil.io.{ZRecordReaderT, ZRecordWriterT}
import com.google.cloud.gszutil.{CopyBook, Decoding, SchemaProvider, Util}
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.ibm.jzos.ZOS.{PDSIterator, RecordIterator}

object IBM extends ZFileProvider with Logging {
  override def init(): Unit = {
    ZOS.addCCAProvider()
    System.setProperty("java.net.preferIPv4Stack" , "true")
    System.out.println("Build Info:\n" + Util.readS("build.txt"))
  }

  override def readDDWithCopyBook(dd: String, copyBook: SchemaProvider): ZRecordReaderT = {
    val rr = ZOS.readDD(dd)

    require(rr.lRecl == copyBook.LRECL, s"Copybook LRECL ${copyBook.LRECL} doesn't match DSN LRECL ${rr.lRecl}")
    rr
  }

  override def exists(dsn: String): Boolean = ZOS.exists(dsn)

  override def ddExists(dd: String): Boolean = ZOS.ddExists(dd)

  override def listPDS(dsn: String): Iterator[PDSMemberInfo] = new PDSIterator(dsn)

  override def readDSN(dsn: String): ZRecordReaderT = ZOS.readDSN(dsn)

  override def readDSNLines(dsn: String): Iterator[String] =
    new RecordIterator(readDSN(dsn)).takeWhile(_ != null)

  override def writeDSN(dsn: String): ZRecordWriterT = ZOS.writeDSN(dsn)

  override def readDD(dd: String): ZRecordReaderT = ZOS.readDD(dd)

  override def readStdin(): String = {
    val in = ByteStreams.toByteArray(System.in)
    new String(in, Decoding.CP1047)
  }

  override def readDDString(dd: String, recordSeparator: String): String = {
    val in = readDD(dd)
    val bytes = Util.readAllBytes(in)
    val decoded = Decoding.ebcdic2ASCIIBytes(bytes)
    Util.records2string(decoded, in.lRecl, Charsets.UTF_8, recordSeparator)
  }

  private var cp: CredentialProvider = _

  override def getCredentialProvider(): CredentialProvider = {
    if (cp != null) cp
    else {
      val bytes = Util.readAllBytes(readDD("KEYFILE"))
      cp = new GoogleCredentialsProvider(bytes)
      cp
    }
  }

  override def loadCopyBook(dd: String): CopyBook = {
    val raw = readDDString(dd, "\n")
    logger.debug(s"Parsing copy book:\n$raw")
    try {
      val copyBook = CopyBook(raw)
      logger.info(s"Loaded copy book:\n$copyBook")
      copyBook
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to parse copybook", e)
    }
  }

  override def jobName: String = ZOS.getJobName
  override def jobDate: String = sys.env.getOrElse("JOBDATE","UNKNOWN")
  override def jobTime: String = sys.env.getOrElse("JOBTIME","UNKNOWN")
  override def getInfo: ZInfo = ZOS.getInfo

  override def getSymbol(s: String): Option[String] = ZOS.getSymbol(s)

  override def substituteSystemSymbols(s: String): String = ZOS.substituteSystemSymbols(s)

  override def submitJCL(jcl: Seq[String]): Option[ZMVSJob] = ZOS.submitJCL(jcl)
}
