package com.google.cloud.gszutil.io

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GCE
import com.google.cloud.gszutil.Util
import com.google.cloud.gszutil.Util.Logging
import org.scalatest.FlatSpec

class GCESpec extends FlatSpec with Logging {
  Util.configureLogging(true)

  "GCE" should "launch VM" in {
    val gce = GCE.defaultClient(GoogleCredentials.getApplicationDefault())
    val instance = GCE.createVM("grecv-"+(System.currentTimeMillis()/1000L),
    "gs://gsztest/gsz.tar",
      "gszutil@token-broker-poc.iam.gserviceaccount.com",
      "token-broker-poc",
      "us-east1-b",
      s"projects/token-broker-poc/regions/us-east1/subnetworks/default",
      gce,
      "n1-standard-4"
    )
    assert(instance.ip.nonEmpty)
    logger.info(JacksonFactory.getDefaultInstance.toPrettyString(instance.instance))
  }

}
