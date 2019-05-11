package com.google.auth.oauth2

import com.google.cloud.gszutil.GSXML.CredentialProvider
import com.google.cloud.hadoop.util.AccessTokenProvider
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

object StaticAccessTokenProvider {
  protected var Instance: CredentialProvider = _
  protected val expired = new AccessTokenProvider.AccessToken("", -1L)
  val TpImpl = "fs.gs.auth.access.token.provider.impl"
  val TpClassName = "com.google.auth.oauth2.StaticAccessTokenProvider"
  val FsImpl = "fs.gs.impl"
  val FsClassName = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

  def setCredentialProvider(cp: CredentialProvider): Unit =
    Instance = cp

  def sparkConf(c: SparkConf = new SparkConf()): SparkConf =
    c.set(TpImpl, TpClassName)
      .set(FsImpl, FsClassName)

  def configure(c: Configuration = new Configuration()): Configuration = {
    c.set(TpImpl, TpClassName)
    c.set(FsImpl, FsClassName)
    c
  }
}

class StaticAccessTokenProvider extends AccessTokenProvider {
  private var token: AccessTokenProvider.AccessToken = StaticAccessTokenProvider.expired

  override def getAccessToken: AccessTokenProvider.AccessToken =
    token

  override def refresh(): Unit = {
    if (token.getExpirationTimeMilliSeconds < System.currentTimeMillis()){
      val newToken = StaticAccessTokenProvider.Instance.getCredentials.refreshAccessToken
      token = new AccessTokenProvider.AccessToken(newToken.getTokenValue, newToken.getExpirationTimeMillis)
    }
  }

  override def setConf(conf: Configuration): Unit = {}

  override def getConf: Configuration =
    new Configuration(false)
}
