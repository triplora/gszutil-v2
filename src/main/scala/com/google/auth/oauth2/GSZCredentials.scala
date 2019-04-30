package com.google.auth.oauth2

import java.net.URI
import com.google.cloud.gszutil.KeyFileProto.KeyFile
import com.google.common.collect.ImmutableList

object GSZCredentials {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"
  val Scopes = ImmutableList.of[String](BigQueryScope,StorageScope)

  def fromKeyFile(keyFile: KeyFile): GoogleCredentials = {
    ServiceAccountCredentials.fromPkcs8(
      keyFile.getClientId,
      keyFile.getClientEmail,
      keyFile.getPrivateKey,
      keyFile.getPrivateKeyId,
      Scopes,
      OAuth2Utils.HTTP_TRANSPORT_FACTORY,
      new URI(keyFile.getTokenUri),
      null,
      keyFile.getProjectId)
  }
}
