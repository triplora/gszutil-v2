/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import java.nio.channels.Channels

import com.google.cloud.gszutil.{CopyBook, Decoding, Util}
import com.google.cloud.gszutil.Util.{CredentialProvider, GoogleCredentialsProvider, Logging}
import com.google.cloud.gszutil.io.{DDChannel, ZChannel, ZRecordReaderT}
import com.google.common.io.ByteStreams

object IBM extends ZFileProvider with Logging {
  override def init(): Unit = {
    ZOS.addCCAProvider()
    System.setProperty("java.net.preferIPv4Stack" , "true")
  }
  override def readChannel(dd: String, copyBook: CopyBook): DDChannel = {
    val rr = ZOS.readDD(dd)
    require(rr.lRecl == copyBook.LRECL)
    DDChannel(new ZChannel(rr), rr.lRecl, rr.blkSize)
  }

  override def ddExists(dd: String): Boolean = ZOS.ddExists(dd)

  override def readDD(dd: String): ZRecordReaderT = ZOS.readDD(dd)

  override def readStdin(): String = {
    val in = ByteStreams.toByteArray(System.in)
    new String(in, Decoding.CP1047)
  }

  override def readDDString(dd: String): String = {
    val in = readDD(dd)
    val bytes = Util.readAllBytes(new ZChannel(in))
    bytes.grouped(in.lRecl)
      .map(new String(_, Decoding.CP1047).trim)
      .mkString("\n")
  }

  override def getCredentialProvider(keyFileDD: String): CredentialProvider = {
    val in = Channels.newInputStream(new ZChannel(readDD(keyFileDD)))
    val bytes = ByteStreams.toByteArray(in)
    new GoogleCredentialsProvider(bytes)
  }

  override def loadCopyBook(dd: String): CopyBook = {
    val copyBook = CopyBook(readDDString(dd))
    logger.info(s"Loaded copy book with LRECL=${copyBook.LRECL} FIELDS=${copyBook.FieldNames.mkString(",")}```\n${copyBook.raw}\n```")
    copyBook
  }
}
