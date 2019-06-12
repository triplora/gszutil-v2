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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.KeyFileCredentialProvider
import com.google.common.io.Resources
import org.scalatest.{BeforeAndAfterAll, FlatSpec}


class KeyFileSpec extends FlatSpec with BeforeAndAfterAll {
  private def readKeyfile = Resources.toString(Resources.getResource("keyfile.json"), StandardCharsets.UTF_8)
  private def readPb: KeyFile = Util.convertJson(new ByteArrayInputStream(readKeyfile.getBytes(StandardCharsets.UTF_8)))

  override def beforeAll(): Unit = {
    Util.configureLogging()
  }

  "bouncy castle" should "create private key" ignore {
    val credential = Util.readCredentials(new ByteArrayInputStream(readKeyfile.getBytes(StandardCharsets.UTF_8)))
    assert(credential.refreshToken())
    System.out.println(credential.getRefreshToken)
  }

  "Util" should "convert keyfile" ignore {
    val keyFile = readPb
    System.out.println(keyFile.getClientEmail)
    System.out.println(keyFile.getPrivateKey)
  }

  it should "write keyfile to pb" ignore {
    val keyFile = Util.convertJson(new ByteArrayInputStream(Resources.toByteArray(Resources.getResource("keyfile.json"))))

    val cp = KeyFileCredentialProvider(keyFile)
    val cred = cp.getCredentials
    cred.refresh()
    val token = cred.getAccessToken.getTokenValue
    System.out.println(token.substring(0,10)+"...")
    val withToken = keyFile.toBuilder.setAccessToken(token).build()

    Files.write(Paths.get("src/main/resources/keyfile.pb"), withToken.toByteArray)
  }
}
