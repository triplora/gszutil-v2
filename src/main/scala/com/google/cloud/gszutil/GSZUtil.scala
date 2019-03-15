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

import java.io.InputStream
import com.ibm.jzos.ZFileConstants.FLAG_DISP_SHR
import com.ibm.jzos.{RecordReader, ZFile}

import scala.util.Try

object GSZUtil {

  def usage(): Unit = {
    val msg = s"Usage: ${this.getClass.getSimpleName} cp <src> gs://<bucket>/<path>"
    System.err.println(msg)
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {
    val dsn = args(1)
    val (bucket, path) = Try(parseUri(args(2))).getOrElse(("",""))
    if (args(0) != "cp" || bucket == "" || path == "" || dsn == "") {
      usage()
    }

    import Credentials._
    val credential = GSXML.PrivateKeyCredentialProvider(privateKeyId, privateKeyPem, serviceAccountId)
    val gcs = GSXML.getClient(credential)

    gcs.putObject(bucket, path, dsnInputStream(dsn))
  }

  def readDSN(dsn: String): RecordReader = {
    val in = ZFile.getSlashSlashQuotedDSN(dsn, true)
    RecordReader.newReader(in, FLAG_DISP_SHR)
  }

  def dsnInputStream(dsn: String): InputStream = {
    new RecordReaderInputStream(readDSN(dsn))
  }

  class RecordReaderInputStream(in: RecordReader) extends InputStream {
    private val byte = new Array[Byte](1)
    override def read(): Int = {
      in.read(byte)
      byte(0)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      in.read(b, off, len)
    }
  }

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


}
