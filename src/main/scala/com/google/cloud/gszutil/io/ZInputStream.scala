package com.google.cloud.gszutil.io

import java.io.InputStream
import java.nio.channels.Channels

import com.google.cloud.gszutil.ZOS

object ZInputStream{
  def apply(dd: String): InputStream =
    Channels.newInputStream(new ZChannel(ZOS.readDD(dd)))
}
