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

package com.google.cloud.gszutil

import java.net.{InetAddress, Socket}

import com.google.api.client.googleapis.GoogleUtils
import com.google.api.client.util.SslUtils
import com.google.cloud.gszutil.Util.Logging
import javax.net.ssl.{SSLContext, SSLSocket, SSLSocketFactory}

/** This SSLSocketFactory implementation is necessary to
  * disable ECDHE ciphers which are not supported by
  * IBM Crypto Cards.
  */
class CCASSLSocketFactory extends SSLSocketFactory with Logging {
  private final val Protocols = Array("TLSv1.2")
  private final val Ciphers = Array(
    "TLS_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_RSA_WITH_AES_256_CBC_SHA",
    "TLS_RSA_WITH_AES_128_CBC_SHA"
  )

  private val factory: SSLSocketFactory = {
    val ciphers: String = Ciphers.mkString(",")
    System.setProperty("jdk.tls.client.cipherSuites", ciphers)
    System.setProperty("jdk.tls.client.protocols" , "TLSv1.2")
    val ctx = SSLContext.getInstance("TLSv1.2")
    val tmf = SslUtils.getPkixTrustManagerFactory
    tmf.init(GoogleUtils.getCertificateTrustStore)
    ctx.init(null, tmf.getTrustManagers, null)
    ctx.getSocketFactory
  }

  override def getDefaultCipherSuites: Array[String] = throw new UnsupportedOperationException

  override def getSupportedCipherSuites: Array[String] = throw new UnsupportedOperationException

  override def createSocket(socket: Socket, host: String, port: Int, autoClose: Boolean): Socket = {
    val s = factory.createSocket(socket, host, port, autoClose)
    s match {
      case x: SSLSocket =>
        x.setEnabledCipherSuites(Ciphers)
        x.setEnabledProtocols(Protocols)
        logger.debug("created " + x.getClass.getCanonicalName + " with " + x.getEnabledProtocols.mkString(",") + " Cipher Suites: "+x.getEnabledCipherSuites.mkString(","))
      case x =>
        logger.warn(s"${x.getClass.getCanonicalName} is not an instance of SSLSocket ")
    }
    s
  }

  override def createSocket(host: String, port: Int): Socket = throw new UnsupportedOperationException

  override def createSocket(host: String, port: Int, localAddress: InetAddress, localPort: Int): Socket = throw new UnsupportedOperationException

  override def createSocket(address: InetAddress, port: Int): Socket = throw new UnsupportedOperationException

  override def createSocket(inetAddress: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket = throw new UnsupportedOperationException
}
