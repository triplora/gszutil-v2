package com.google.cloud.gszutil.io

import java.io.InputStream
import java.nio.channels.Channels

import com.ibm.jzos.CrossPlatform

object ZInputStream{
  def apply(dd: String): InputStream =
    Channels.newInputStream(new ZChannel(CrossPlatform.readDD(dd)))

  def apply(reader: ZRecordReaderT): InputStream =
    Channels.newInputStream(new ZChannel(reader))
}
