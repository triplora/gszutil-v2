/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.bqsh.cmd

import com.google.cloud.bqsh.{ArgParser, Command, GsUtilConfig, GsZUtilConfig, GsZUtilOptionParser}
import com.google.cloud.gszutil.{CopyBook, SchemaProvider}
import com.google.cloud.imf.grecv.client.GRecvClient
import com.google.cloud.imf.gzos.{CloudDataSet, DataSetInfo, MVS, MVSStorage}
import com.google.cloud.imf.util.{Logging, Services}

/** Command-Line utility used to request remote transcoding of
  * mainframe datasets in Cloud Storage
  * specify --inDsn or set INFILE DD
  * output location specified by --gcsOutUri or GCSOUTURI environment variable
  */
object GsZUtil extends Command[GsZUtilConfig] with Logging {
  override val name: String = "gszutil"
  override val parser: ArgParser[GsZUtilConfig] = GsZUtilOptionParser
  override def run(c: GsZUtilConfig, zos: MVS, env: Map[String,String]): Result = {
    val sp: SchemaProvider =
      if (c.cobDsn.nonEmpty) {
        logger.info(s"reading copybook from DSN=${c.cobDsn}")
        CopyBook(zos.readDSNLines(MVSStorage.parseDSN(c.cobDsn)).mkString("\n"))
      } else {
        logger.info(s"reading copybook from DD:COPYBOOK")
        zos.loadCopyBook("COPYBOOK")
      }
    //TODO read FLDINFO DD and merge field info

    val creds = zos.getCredentialProvider().getCredentials
    val gcs = Services.storage(creds)

    val cpConfig = GsUtilConfig(
      schemaProvider = Option(sp),
      remote = true,
      replace = true,
      remoteHost = c.remoteHost,
      remotePort = c.remotePort,
      gcsUri = c.gcsOutUri
    )

    val dsInfo: DataSetInfo = {
      if (c.inDsn.nonEmpty) {
        logger.info(s"using DSN=${c.inDsn} from --inDsn command-line option")
        DataSetInfo(dsn = c.inDsn)
      } else {
        zos.dsInfo("INFILE") match {
          case Some(ds) =>
            logger.info(s"using DSN=${ds.dsn} from DD:INFILE")
            ds
          case None =>
            val msg = "input DSN not set. provide --inDsn command-line option or" +
              " INFILE DD"
            logger.error(msg)
            throw new RuntimeException(msg)
        }
      }
    }

    CloudDataSet.readCloudDD(gcs, "INFILE", dsInfo) match {
      case Some(in) =>
        logger.info(s"CloudDataSet found for DSN=${dsInfo.dsn}")
        GRecvClient.run(cpConfig, zos, in, sp, GRecvClient)
      case None =>
        logger.error(s"CloudDataSet not found for DSN=${dsInfo.dsn}")
        Result.Failure(s"DSN ${c.inDsn} not found")
    }
  }
}
