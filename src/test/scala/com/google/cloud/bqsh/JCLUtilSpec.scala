package com.google.cloud.bqsh

import org.scalatest.FlatSpec

class JCLUtilSpec extends FlatSpec {
  "JCLUtil" should "match regex" in {
    assert("TDUS123".matches("^TD.*$"))
  }
}
