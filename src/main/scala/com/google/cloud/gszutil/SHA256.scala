package com.google.cloud.gszutil

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZChannel
import com.google.common.hash.Hashing

object SHA256 extends Logging {
  def apply(dd: String): String = {
    val in = ZChannel(dd)
    val h = Hashing.sha256().newHasher()
    val buf = ByteBuffer.allocate(4096)
    var i = 0
    while (in.read(buf) >= 0){
      in.read(buf)
      buf.flip()
      h.putBytes(buf)
      buf.clear()
      i += 1
    }
    if (i % 100 == 0)
      logger.info(s"i=$i")
    h.hash().toString
  }
}
