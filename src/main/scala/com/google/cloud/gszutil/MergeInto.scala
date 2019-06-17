package com.google.cloud.gszutil

import com.google.cloud.gszutil.Util.CredentialProvider

object MergeInto {
  def run(c: Config, cp: CredentialProvider): Unit = {
    val bq = BQ.defaultClient(c.bqProject, c.bqLocation, cp.getCredentials)

  }
}
