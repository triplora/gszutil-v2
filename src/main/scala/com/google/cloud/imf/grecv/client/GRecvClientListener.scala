package com.google.cloud.imf.grecv.client

import java.net.URI
import java.nio.ByteBuffer

import com.google.auth.oauth2.OAuth2Credentials
import com.google.cloud.imf.gzos.pb.GRecvProto.GRecvRequest
import com.google.cloud.imf.util.Logging
import io.grpc.okhttp.OkHttpChannelBuilder


/** Sends bytes to server, maintaining a hash of all bytes sent */
class GRecvClientListener(cred: OAuth2Credentials,
                          cb: OkHttpChannelBuilder,
                          request: GRecvRequest,
                          baseUri: URI,
                          bufSz: Int,
                          partLimit: Long) extends Logging {
  val buf: ByteBuffer = ByteBuffer.allocate(bufSz)

  def newObj(): TmpObj = {
    cred.refreshIfExpired()
    new TmpObj(
      baseUri.getAuthority,
      baseUri.getPath.stripPrefix("/").stripSuffix("/") + "/tmp/",
      cred.getAccessToken, cb, request, partLimit, compress = true)
  }
  private var obj: TmpObj = _

  def getWriter(): TmpObj = {
    if (obj == null || obj.isClosed) obj = newObj()
    obj
  }

  def flush(): Unit = {
    buf.flip()
    getWriter().write(buf)
  }

  def close(): Unit = {
    if (obj != null) {
      obj.close()
      obj = null
    }
  }
}

