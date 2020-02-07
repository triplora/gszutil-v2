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

import com.google.cloud.gszutil.{CopyBook, SchemaProvider}
import com.google.cloud.gszutil.Util.{CredentialProvider, PDSMemberInfo, ZInfo, ZMVSJob}
import com.google.cloud.gszutil.io.{ZRecordReaderT, ZRecordWriterT}

trait ZFileProvider {
  def init(): Unit

  /** Opens a ReadableByteChannel
    *
    * @param dd DD name of input dataset
    * @param copyBook SchemaProvider used to verify LRECL of the dataset
    * @return
    */
  def readDDWithCopyBook(dd: String, copyBook: SchemaProvider): ZRecordReaderT
  def ddExists(dd: String): Boolean
  def exists(dsn: String): Boolean
  def readDSN(dsn: String): ZRecordReaderT
  def readDSNLines(dsn: String): Iterator[String]
  def writeDSN(dsn: String): ZRecordWriterT
  def readDD(dd: String): ZRecordReaderT
  def readStdin(): String
  def readDDString(dd: String, recordSeparator: String): String
  def getCredentialProvider(): CredentialProvider
  def listPDS(dsn: String): Iterator[PDSMemberInfo]
  def loadCopyBook(dd: String): CopyBook
  def jobName: String
  def jobDate: String
  def jobTime: String
  def jobId: String = jobName+jobDate+jobTime
  def getInfo: ZInfo
  def getSymbol(s: String): Option[String]
  def substituteSystemSymbols(s: String): String
  def submitJCL(jcl: Seq[String]): Option[ZMVSJob]
}

