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
package com.google.cloud.bqsh

import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.gszutil.io.ZDataSet

object GsUtilConfig {
  /** Minimal constructor */
  def createLocal(sourceDD: String,
                  schemaProvider: SchemaProvider,
                  destinationUri: String,
                  projectId: String,
                  datasetId: String,
                  location: String): GsUtilConfig = {
    GsUtilConfig(source = sourceDD,
                 schemaProvider = Option(schemaProvider),
                 destinationUri = destinationUri,
                 projectId = projectId,
                 datasetId = datasetId,
                 location = location,
                 replace = true)
  }

  def createRemote(sourceDD: String,
             schemaProvider: SchemaProvider,
             destinationUri: String,
             projectId: String,
             datasetId: String,
             location: String,
             pkgUri: String,
             zone: String,
             subnet: String,
             remotePort: Int,
             serviceAccount: String): GsUtilConfig = {
    GsUtilConfig(source = sourceDD,
      schemaProvider = Option(schemaProvider),
      destinationUri = destinationUri,
      projectId = projectId,
      datasetId = datasetId,
      location = location,
      pkgUri = pkgUri,
      zone = zone,
      subnet = subnet,
      remotePort = remotePort,
      serviceAccount = serviceAccount,
      remote = true,
      replace = true)
  }

  def createRemote2(sourceDD: String,
                    schemaProvider: SchemaProvider,
                    destinationUri: String,
                    projectId: String,
                    datasetId: String,
                    location: String,
                    remoteHostname: String,
                    remotePort: Int): GsUtilConfig = {
    GsUtilConfig(source = sourceDD,
      schemaProvider = Option(schemaProvider),
      destinationUri = destinationUri,
      projectId = projectId,
      datasetId = datasetId,
      location = location,
      remoteHost = remoteHostname,
      remotePort = remotePort,
      remote = true,
      replace = true)
  }
}

case class GsUtilConfig(source: String = "INFILE",
                        copyBook: String = "COPYBOOK",
                        keyFile: String = "KEYFILE",
                        destinationUri: String = "",
                        mode: String = "",
                        replace: Boolean = false,
                        recursive: Boolean = false,
                        compressBuffer: Int = 32*1024,
                        maxErrorPct: Double = 0,
                        blocksPerBatch: Int = 128,
                        partSizeMB: Int = 256,
                        parallelism: Int = 4,
                        timeOutMinutes: Int = -1,

                        // Global
                        projectId: String = "",
                        datasetId: String = "",
                        location: String = "US",

                        // Custom
                        schemaProvider: Option[SchemaProvider] = None,
                        allowNonAscii: Boolean = false,
                        statsTable: String = "",
                        remote: Boolean = false,
                        remoteHost: String = "",
                        remotePort: Int = 51770,
                        tls: Boolean = false,
                        blocks: Long = 1024,
                        nConnections: Int = 4,
                        pkgUri: String = "",
                        zone: String = "",
                        subnet: String = "",
                        serviceAccount: String = "",
                        machineType: String = "n1-standard-4",
                        testInput: Option[ZDataSet] = None
)
