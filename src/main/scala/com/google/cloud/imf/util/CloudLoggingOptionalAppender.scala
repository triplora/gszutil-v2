package com.google.cloud.imf.util

import com.google.cloud.imf.gzos.Util
import org.apache.log4j.spi.{ErrorHandler, Filter, LoggingEvent}
import org.apache.log4j.varia.NullAppender
import org.apache.log4j.{Appender, Layout}

import scala.sys.env
import scala.util.Try

class CloudLoggingOptionalAppender extends Appender {
  private val maybeCloudAppender: Option[CloudLoggingAppender] = for {
    projectId <- env.get("LOG_PROJECT").filter(_.nonEmpty)
    logId <- env.get("LOG_ID").filter(_.nonEmpty)
    cred <- Try(Util.zProvider.getCredentialProvider().getCredentials).toOption
  } yield {
    System.out.println(s"Cloud Logging starts for projectId=$projectId and logId=$logId")
    new CloudLoggingAppender(projectId, logId, cred)
  }

  private val appender: Appender = maybeCloudAppender.getOrElse({
    System.out.println("Environment variables LOG_PROJECT or LOG_ID are blank, Cloud Logging is disabled.");
    new NullAppender
  })

  override def addFilter(newFilter: Filter): Unit = appender.addFilter(newFilter)

  override def getFilter: Filter = appender.getFilter

  override def clearFilters(): Unit = appender.clearFilters()

  override def close(): Unit = appender.close()

  override def doAppend(event: LoggingEvent): Unit = appender.doAppend(event)

  override def getName: String = appender.getName

  override def setErrorHandler(errorHandler: ErrorHandler): Unit = appender.setErrorHandler(errorHandler)

  override def getErrorHandler: ErrorHandler = appender.getErrorHandler

  override def setLayout(layout: Layout): Unit = appender.setLayout(layout)

  override def getLayout: Layout = appender.getLayout

  override def setName(name: String): Unit = appender.setName(name)

  override def requiresLayout(): Boolean = appender.requiresLayout()
}
