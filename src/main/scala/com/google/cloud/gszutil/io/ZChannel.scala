/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gszutil.io

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class ZChannel(private val reader: ZRecordReaderT) extends ReadableByteChannel {
  private var hasRemaining = true
  private var open = true
  private val data: Array[Byte] = new Array[Byte](reader.blkSize)
  private val buf: ByteBuffer = ByteBuffer.wrap(data)
  private var bytesRead: Long = 0

  buf.position(buf.capacity) // initial buffer state = empty

  override def read(dst: ByteBuffer): Int = {
    if (buf.remaining < dst.capacity && hasRemaining){
      buf.compact()
      val n = reader.read(data, buf.position, buf.remaining)
      if (n > 0) {
        buf.position(buf.position + n)
        bytesRead += n
      } else if (n < 0){
        reader.close()
        hasRemaining = false
      }
      buf.flip()
    }
    val n = math.min(buf.remaining, dst.remaining)
    dst.put(data, buf.position, n)
    buf.position(buf.position + n)
    if (hasRemaining || n > 0) {
      n
    } else {
      close()
      -1
    }
  }

  override def isOpen: Boolean = open
  override def close(): Unit = {
    if (isOpen) {
      reader.close()
      open = false
    }
  }
  def getBytesRead: Long = bytesRead
}
