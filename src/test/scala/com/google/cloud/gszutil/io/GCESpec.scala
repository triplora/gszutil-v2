package com.google.cloud.gszutil.io

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.GCE
import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.util.Logging
import org.scalatest.flatspec.AnyFlatSpec

class GCESpec extends AnyFlatSpec with Logging {
  Util.configureLogging(true)

  "GCE" should "launch VM" in {
    val gce = GCE.defaultClient(GoogleCredentials.getApplicationDefault)
    val instance = GCE.createVM("grecv-"+(System.currentTimeMillis/1000L),
      "grecv",
      Map("pkguri" -> "gs://gszutil-test/pkg"),
      "gszutil@pso-wmt-dl.iam.gserviceaccount.com",
      "pso-wmt-dl",
      "us-east1-b",
      s"projects/pso-wmt-dl/regions/us-east1/subnetworks/default",
      gce,
      "n1-standard-4")
    assert(instance.ipAddr.nonEmpty)
    logger.info(instance)
  }

}
