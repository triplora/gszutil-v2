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
