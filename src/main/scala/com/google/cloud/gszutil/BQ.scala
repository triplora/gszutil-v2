package com.google.cloud.gszutil

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.cloud.http.HttpTransportOptions
import org.threeten.bp.Duration

object BQ {
  def defaultClient(project: String, location: String, credentials: Credentials): BigQuery = {
    BigQueryOptions.newBuilder()
      .setLocation(location)
      .setProjectId(project)
      .setCredentials(credentials)
      .setTransportOptions(HttpTransportOptions.newBuilder()
        .setHttpTransportFactory(new CCATransportFactory)
        .build())
      .setRetrySettings(RetrySettings.newBuilder()
        .setMaxAttempts(8)
        .setTotalTimeout(Duration.ofMinutes(30))
        .setInitialRetryDelay(Duration.ofSeconds(8))
        .setMaxRetryDelay(Duration.ofSeconds(32))
        .setRetryDelayMultiplier(2.0d)
        .build())
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "gszutil-0.1"))
      .build()
      .getService
  }
}
