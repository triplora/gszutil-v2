package com.google.cloud.gszutil

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.{Storage, StorageOptions}
import org.threeten.bp.Duration

object GCS {
  def defaultClient(credentials: Credentials): Storage = {
    StorageOptions.newBuilder()
      .setCredentials(credentials)
      .setTransportOptions(HttpTransportOptions.newBuilder()
        .setHttpTransportFactory(new CCATransportFactory)
        .build())
      .setRetrySettings(RetrySettings.newBuilder()
        .setMaxAttempts(100)
        .setTotalTimeout(Duration.ofMinutes(30))
        .setInitialRetryDelay(Duration.ofSeconds(2))
        .setMaxRetryDelay(Duration.ofSeconds(30))
        .setRetryDelayMultiplier(2.0d)
        .build())
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "gszutil-0.1"))
      .build()
      .getService
  }
}
