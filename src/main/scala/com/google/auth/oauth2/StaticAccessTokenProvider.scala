package com.google.auth.oauth2

import com.google.cloud.gszutil.GSXML.CredentialProvider
import com.google.cloud.hadoop.util.AccessTokenProvider
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

object StaticAccessTokenProvider {
  val Config = "fs.gs.auth.access.token.provider.impl"
  val ClassName = "com.google.auth.oauth2.StaticAccessTokenProvider"
  protected var Instance: CredentialProvider = _
  protected val expired = new AccessTokenProvider.AccessToken("", -1L)
  def setCredentialProvider(cp: CredentialProvider): Unit =
    Instance = cp
  def sparkConf(c: SparkConf = new SparkConf()): SparkConf =
    c.set(Config, ClassName)
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
