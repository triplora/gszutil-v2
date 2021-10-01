package com.google.cloud.gszutil.io.exports

import com.google.api.gax.rpc.ServerStreamingCallable
import com.google.cloud.bigquery.storage.v1.{ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.imf.util.Logging

import scala.annotation.tailrec
import scala.util.{Failure, Random, Success, Try}

class AvroRowsRetryableIterator(callable: ServerStreamingCallable[ReadRowsRequest, ReadRowsResponse],
                                request: ReadRowsRequest,
                                retryCount: Int = 5) extends Iterator[ReadRowsResponse] with Logging {
  private var internalIterator: java.util.Iterator[ReadRowsResponse] = callable.call(request).iterator()
  private var offset: Long = request.getOffset
  private var counter = retryCount

  @tailrec
  override final def hasNext: Boolean = Try(internalIterator.hasNext) match {
    case Success(value) => value
    case Failure(ex) if counter < 0 => throw ex
    case Failure(ex) =>
      logger.warn(s"Retrying hasNext(), offset=${request.getOffset}, retry_from_offset=$offset, exception=$ex")
      resetIteratorWithDelay()
      hasNext
  }

  @tailrec
  override final def next: ReadRowsResponse = Try(internalIterator.next()) match {
    case Success(value) => value
    case Failure(ex) if counter < 0 => throw ex
    case Failure(ex) =>
      logger.warn(s"Retrying next(), offset=${request.getOffset}, retry_from_offset=$offset, exception=$ex")
      resetIteratorWithDelay()
      next
  }

  def consumed(rowsCount: Long): Unit = {
    offset += rowsCount
  }

  private def resetIteratorWithDelay(): Unit = {
    counter = counter - 1
    internalIterator = callable.call(request.toBuilder.setOffset(offset).build()).iterator()
    Try(Thread.sleep(Random.between(1, 5) * 1000))
  }
}