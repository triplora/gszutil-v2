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

import com.google.cloud.gszutil.GSXML.XMLStorage
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.cloud.gszutil.Util.{AccessTokenCredentialProvider, PBCredentialProvider}
import com.google.common.io.Resources
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.JavaConverters._

class GSXMLSpec extends FlatSpec with BeforeAndAfterAll {
  private def readKeyfile = Resources.toString(Resources.getResource("keyfile.json"), StandardCharsets.UTF_8)
  private def readPb: KeyFile = Util.convertJson(new ByteArrayInputStream(readKeyfile.getBytes(StandardCharsets.UTF_8)))

  override def beforeAll(): Unit = {
    Util.configureBouncyCastleProvider()
    Util.configureLogging()
  }

  "bouncy castle" should "create private key" in {
    val credential = Util.readCredentials(new ByteArrayInputStream(readKeyfile.getBytes(StandardCharsets.UTF_8)))
    assert(credential.refreshToken())
    System.out.println(credential.getRefreshToken)
  }

  "Util" should "convert keyfile" in {
    val keyFile = readPb
    System.out.println(keyFile.getClientEmail)
    System.out.println(keyFile.getPrivateKey)
  }

  it should "write keyfile to pb" in {
    val keyFile = Util.convertJson(new ByteArrayInputStream(Resources.toByteArray(Resources.getResource("wmt-keyfile.json"))))
    Files.write(Paths.get("wmt-keyfile.pb"), keyFile.toByteArray)

    val cp = PBCredentialProvider(keyFile.toByteArray)
    val cred = cp.getCredential
    cred.refreshToken()
    val token = cred.getAccessToken
    System.out.println(token)
    val withToken = keyFile.toBuilder.setAccessToken(token).build()

    Files.write(Paths.get("wmt-accesstoken.pb"), withToken.toByteArray)
  }

  "GSXML" should "upload" in {
    val bucket = sys.env("BUCKET")
    val pb = readPb

    val cp = PBCredentialProvider(pb.toByteArray)
    val cred = cp.getCredential
    cred.refreshToken()
    val token = cred.getAccessToken
    System.out.println(token)
    val atcp = AccessTokenCredentialProvider(token)
    val gcs = XMLStorage(atcp)

    for (i <- 0 until 3){
      val objectName = s"test_$i"
      val data = new ByteArrayInputStream(s"$i".getBytes(StandardCharsets.UTF_8))
      val putRequest = gcs.putObject(bucket, objectName, data)
      val resp2 = putRequest.execute()
      assert(resp2.getStatusCode == 200)
    }

    val list = gcs.listBucketRecursive(bucket, maxKeys = 2)
    val keys = list.flatMap(_.Contents.asScala.map(_.Key)).toSet
    assert(keys.nonEmpty)
    for (i <- 0 until 3){
      assert(keys.contains(s"test_$i"))
    }
  }
}
