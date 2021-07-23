package com.google.cloud.imf.util

import scala.util.Try

object RetryHelper extends Logging {
  def retryable[A](f: => A, logMessage: String = "", attempts: Int = 3, sleep: Int = 500): Either[Throwable, A] = {
    var count = 0
    def call(): Either[Throwable, A] = {
      Try(f).toEither match {
        case Left(e) =>
          count += 1
          if(count <= attempts) {
            Try(Thread.sleep(sleep))
            logger.error(s"$logMessage. Failed to execute function, will be retried $count time", e)
            call()
          } else Left(e)
        case Right(value) => Right(value)
      }
    }
    call()
  }
}


