package com.google.cloud.imf.util

import java.net.{InetAddress, ServerSocket}
import java.security.cert.X509Certificate
import java.security.spec.RSAKeyGenParameterSpec
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, SecureRandom, Security}

import com.google.cloud.gszutil.CCASSLSocketFactory
import com.typesafe.sslconfig.ssl.FakeKeyStore
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder, NettyServerBuilder}
import io.netty.handler.ssl.{ClientAuth, SslContextBuilder, SslProvider}
import javax.net.ssl.{KeyManager, SSLContext, SSLServerSocket, SSLSocketFactory, TrustManager, X509KeyManager}

object TLSUtil extends Logging {
  def printChain(chain: Array[X509Certificate]): Unit = {
    val msgs = chain.map{c =>
        s"""${c.getSigAlgName} X.509 Certificate ${c.getVersion} ${c.getSerialNumber}
         | Subject ${c.getSubjectDN.getName}
         | Issuer ${c.getIssuerDN.getName}
         | Valid ${c.getNotBefore} to ${c.getNotAfter}""".stripMargin
    }
    logger.info(msgs.mkString("Certificate Chain\n","\n","\n"))
  }

  def tlsSocket(crt: String, pem: String, host: String, port: Int, backlog: Int): ServerSocket =
    tlsSocket(SecurityUtils.getKeyManager(crt, pem), host, port, backlog)

  def tlsSocket(host: String, port: Int, backlog: Int): ServerSocket = {
    val kp = genKey
    val keyManager = new StaticKeyManager(Array[X509Certificate](FakeKeyStore.createSelfSignedCertificate(kp)), kp.getPrivate)
    System.out.println("generated self-signed certificate")
    tlsSocket(keyManager, host, port, backlog)
  }

  def tlsSocket(keyManager: X509KeyManager, host: String, port: Int, backlog: Int): ServerSocket = {
    val ctx = SSLContext.getInstance("TLSv1.2")
    ctx.init(Array[KeyManager](keyManager), Array[TrustManager](new TrustAllX509TrustManager), new SecureRandom())
    val s = ctx.getServerSocketFactory.createServerSocket(port, backlog, InetAddress.getByName(host))
    s.asInstanceOf[SSLServerSocket].setEnabledCipherSuites(CCASSLSocketFactory.Ciphers)
    s
  }

  def selfSigned: (PrivateKey,Array[X509Certificate]) = {
    val kp = genKey
    (kp.getPrivate, Array(FakeKeyStore.createSelfSignedCertificate(kp)))
  }

  def genKey: KeyPair = {
    val g = KeyPairGenerator.getInstance("RSA")
    g.initialize(new RSAKeyGenParameterSpec(2048, RSAKeyGenParameterSpec.F4))
    g.generateKeyPair
  }

  private var sslSocketFactory: SSLSocketFactory = _

  def getDefaultSSLSocketFactory: SSLSocketFactory = {
    if (sslSocketFactory == null) {
      sslSocketFactory = getSocketFactory
      logger.debug(s"initialized socket factory ${sslSocketFactory.getClass.getCanonicalName}")
    }
    sslSocketFactory
  }

  private def getSocketFactory: SSLSocketFactory = {
    val ctx = SSLContext.getInstance("TLSv1.2")
    ctx.init(null, Array[TrustManager](new TrustAllX509TrustManager), new SecureRandom)
    ctx.getSocketFactory
  }

  def addConscryptServer(cb: NettyServerBuilder,
                         key: PrivateKey,
                         chain: Array[X509Certificate]): NettyServerBuilder = {
    cb.sslContext(
      GrpcSslContexts.configure(
        SslContextBuilder
          .forServer(key, chain:_*)
          .sslContextProvider(Security.getProvider("Conscrypt"))
          .clientAuth(ClientAuth.NONE),
        SslProvider.JDK
      ).build
    )
  }

  def addConscrypt(cb: NettyChannelBuilder): NettyChannelBuilder = {
    cb.sslContext(
      GrpcSslContexts.configure(
        SslContextBuilder.forClient
          .sslContextProvider(Security.getProvider("Conscrypt"))
          .trustManager(new TrustAllX509TrustManager)
          .clientAuth(ClientAuth.NONE),
        SslProvider.JDK
      ).build
    )
  }
}
