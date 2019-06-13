package com.google.cloud.gszutil

import org.scalatest.FlatSpec

class ConfigSpec extends FlatSpec {
  "ConfigParser" should "parse" in {
    val args = "load bqProject bqDataset bqTable bucket prefix -p 10"
    val config = ConfigParser.parse(args.split(" "))
    assert(config.isDefined)
    config match {
      case Some(c) =>
        assert(c.parallelism == 10)
    }
  }

}
