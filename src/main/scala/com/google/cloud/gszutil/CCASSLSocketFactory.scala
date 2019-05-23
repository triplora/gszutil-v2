package com.google.cloud.gszutil

import java.net.{InetAddress, Socket}

import javax.net.ssl.{SSLContext, SSLSocketFactory}

class CCASSLSocketFactory extends SSLSocketFactory {
  private val isIBM: Boolean = System.getProperty("java.vm.vendor").contains("IBM")

  private val factory: SSLSocketFactory =
    if (isIBM)
      new com.ibm.jsse2.SSLSocketFactoryImpl()
    else
      SSLContext.getInstance("TLSv1.2").getSocketFactory

  private val ciphers: Array[String] =
    if (isIBM)
      Array[String](
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
      )
    else factory.getSupportedCipherSuites

  override def getDefaultCipherSuites: Array[String] = ciphers

  override def getSupportedCipherSuites: Array[String] = ciphers

  override def createSocket(socket: Socket, s: String, i: Int, b: Boolean): Socket = factory.createSocket(socket, s, i, b)

  override def createSocket(s: String, i: Int): Socket = factory.createSocket(s, i)

  override def createSocket(s: String, i: Int, inetAddress: InetAddress, i1: Int): Socket = factory.createSocket(s, i, inetAddress, i1)

  override def createSocket(inetAddress: InetAddress, i: Int): Socket = factory.createSocket(inetAddress, i)

  override def createSocket(inetAddress: InetAddress, i: Int, inetAddress1: InetAddress, i1: Int): Socket = factory.createSocket(inetAddress, i, inetAddress1, i1)
}
