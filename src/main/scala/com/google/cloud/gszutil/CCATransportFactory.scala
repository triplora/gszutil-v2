package com.google.cloud.gszutil

import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.auth.http.HttpTransportFactory

class CCATransportFactory extends HttpTransportFactory{
  override def create(): HttpTransport =
    new NetHttpTransport.Builder()
      .setSslSocketFactory(new CCASSLSocketFactory())
      .build()
}
