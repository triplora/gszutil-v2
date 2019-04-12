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

import com.google.cloud.gszutil.GSXML.{DefaultCredentialProvider, PrivateKeyCredentialProvider, getClient}
import com.ibm.jzos.{RecordReader, ZFile, ZFileConstants}

import scala.util.{Success, Try}

object GSZUtil {
  case class Config(dsn: String = "",
                    dest: String = "",
                    mode: String = "")

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
              c.copy(dest = x)
            }
            .validate{x =>
              Try(parseUri(x)) match {
                case Success((bucket, path)) if bucket.length > 2 && path.nonEmpty =>
                  success
                case _ =>
                  failure(s"'$x' is not a valid GCS path")
              }
            }
            .text("dest is the destination path gs://bucket/path")
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
    val gcs1 = Try(getClient(DefaultCredentialProvider))

    System.out.println(System.getProperty("user.home"))
    System.out.println(gcs1)
    val gcs2 = Try(getClient(PrivateKeyCredentialProvider(
        """-----BEGIN PRIVATE KEY-----\n<redacted>\n-----END PRIVATE KEY-----\n""".replaceAllLiterally("\\n", "\n"),
        "serviceaccount@project.iam.gserviceaccount.com")))

    System.out.println(gcs2)

    val (bucket, path) = parseUri(config.dest)
    val data = dsnInputStream(config.dsn)
    gcs2.get.putObject(bucket, path, data)
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
