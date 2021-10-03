package com.google.cloud.gszutil.io.exports

import com.google.api.gax.rpc.{ServerStream, ServerStreamingCallable}
import com.google.cloud.bigquery.storage.v1.{ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.imf.util.Logging

import scala.annotation.tailrec
import scala.util.{Failure, Random, Success, Try}

class AvroRowsRetryableIterator(callable: ServerStreamingCallable[ReadRowsRequest, ReadRowsResponse],
                                request: ReadRowsRequest,
                                retryCount: Int = 5) extends Iterator[ReadRowsResponse] with Logging {
  private var internalStream: ServerStream[ReadRowsResponse] = callable.call(request)
  private var internalIterator: java.util.Iterator[ReadRowsResponse] = internalStream.iterator()
  private var offset: Long = request.getOffset
  private var counter = retryCount - 1

  @tailrec
  override final def hasNext: Boolean = Try(internalIterator.hasNext) match {
    case Success(value) => value
    case Failure(ex) if counter < 0 => throw ex
    case Failure(ex) =>
      logger.info(s"Retrying hasNext(), offset=${request.getOffset}, retry_from_offset=$offset, exception=$ex")
      resetIteratorWithDelay()
      hasNext
  }

  @tailrec
  override final def next: ReadRowsResponse = Try(internalIterator.next()) match {
    case Success(value) => value
    case Failure(ex) if counter < 0 => throw ex
    case Failure(ex) =>
      logger.info(s"Retrying next(), offset=${request.getOffset}, retry_from_offset=$offset, exception=$ex")
      resetIteratorWithDelay()
      next
  }

  def consumed(rowsCount: Long): Unit = {
    offset += rowsCount
  }

  private def resetIteratorWithDelay(): Unit = {
    Try(Thread.sleep(Random.between(1, 5) * 1000))
    Try(internalStream.cancel()) match {
      case Failure(exception) => logger.error(s"Could not close read stream ${request.getReadStream}, error $exception")
      case _ => //do nothing
    }
    counter = counter - 1
    internalStream = callable.call(request.toBuilder.setOffset(offset).build())
    internalIterator = internalStream.iterator()
  }
}