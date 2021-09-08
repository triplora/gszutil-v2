package com.google.cloud.imf.util

import org.scalatest.flatspec.AnyFlatSpec
import com.google.cloud.imf.util.RetryHelper._

class RetryHelperSpec extends AnyFlatSpec {

  var calls = 0
  def errorFunc(): Unit = {
    calls += 1
    println("function called")
    if(calls <= 4)
    throw new IllegalStateException()
  }

  "retryable" should "return left after 4 attempts" in {
    val res = retryable(errorFunc())
    assert(calls == 4)
    assert(res.isLeft)
  }

  "retryable" should "return right after 5 attempts" in {
    val res = retryable(errorFunc(), attempts = 5)
    assert(calls == 5)
    assert(res.isRight)
  }
}
