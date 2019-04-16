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

import java.io.{ByteArrayInputStream, InputStream}

import com.google.cloud.gszutil.GSXML.{DefaultCredentialProvider, FileCredentialProvider, PrivateKeyCredentialProvider, getClient}
import com.google.common.base.Charsets
import com.google.common.io.CharStreams
import com.ibm.jzos.{FileFactory, RecordReader, ZFile, ZFileConstants}

import scala.util.{Success, Try}

object GSZUtil {
  case class Config(dsn: String = "",
                    dest: String = "",
                    keyfile: String = "",
                    bucket: String = "",
                    path: String = "",
                    mode: String = "",
                    useBCProv: Boolean = true,
                    debug: Boolean = true)

  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("GSZUtil") {
      head("GSZUtil", "0.1")

      cmd("cp")
        .action{(_, c) => c.copy(mode = "cp")}
        .text("cp is a command.")
        .children(
          arg[String]("dsn")
            .required()
            .action{(x, c) => c.copy(dsn = x)}
            .text("dsn is the file to be copied"),
          arg[String]("dest")
            .required()
            .action{(x, c) =>
              Try(parseUri(x)) match {
                case Success((bucket, path)) =>
                  c.copy(dest = x, bucket = bucket, path = path)
                case _ =>
                  c.copy(dest = x)
              }
            }
            .text("dest is the destination path gs://bucket/path"),
          arg[String]("keyfile")
            .required()
            .action{(x, c) => c.copy(keyfile = x)}
            .text("path to json credentials keyfile")
        )
      checkConfig(c =>
        if (c.bucket.isEmpty || c.path.isEmpty)
          failure(s"invalid destination '${c.dest}'")
        else success
      )
    }

  def main(args: Array[String]): Unit = {
    Parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
        System.out.println(s"Invalid args: ${args.mkString(" ")}")
    }
  }

  def run(config: Config): Unit = {
    if (config.debug) Util.printDebugInformation()
    if (config.useBCProv) Util.configureBouncyCastleProvider()

    val gcs0 = Try(getClient(new FileCredentialProvider(config.keyfile)))
    val gcs1 = Try(getClient(DefaultCredentialProvider))
    val gcs2 = Try(getClient(PrivateKeyCredentialProvider(
        """-----BEGIN PRIVATE KEY-----\n<redacted>\n-----END PRIVATE KEY-----\n""".replaceAllLiterally("\\n", "\n"),
        "serviceaccount@project.iam.gserviceaccount.com")))

    System.out.println("\n\nFileCredentialProvider")
    Util.printException(gcs0)

    System.out.println("\n\nDefaultCredentialProvider")
    Util.printException(gcs1)

    System.out.println("\n\nPrivateKeyCredentialProvider")
    Util.printException(gcs2)

    gcs0
      .orElse(gcs1)
      .orElse(gcs2)
      .foreach{gcs =>
      val data = dsnInputStream(config.dsn)
      val request = gcs.putObject(config.bucket, config.path, data)
      System.out.println(s"Uploading ${config.dsn} to ${config.dest}")
      val response = request.execute()
      if (response.isSuccessStatusCode){
        System.out.println(s"Success")
      } else {
        System.out.println(s"Error: Status code ${response.getStatusCode}")
        System.out.println(s"${response.parseAsString}")
      }
    }
  }

  def readUtf8(path: String): InputStream = {
    val reader = FileFactory.newBufferedReader(path, "UTF-8")
    val bytes = CharStreams.toString(reader).getBytes(Charsets.UTF_8)
    new ByteArrayInputStream(bytes)
  }

  def readDSN(dsn: String): RecordReader = {
    val in = ZFile.getSlashSlashQuotedDSN(dsn, false)
    RecordReader.newReader(in, ZFileConstants.FLAG_DISP_SHR)
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
