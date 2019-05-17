package com.google.cloud.gszutil.io

import java.io.InputStream
import java.nio.channels.Channels

import com.ibm.jzos.ZOS

object ZInputStream{
  def apply(dd: String): InputStream =
    Channels.newInputStream(new ZChannel(ZOS.readDD(dd)))

  def apply(reader: ZRecordReaderT): InputStream =
    Channels.newInputStream(new ZChannel(reader))
}
