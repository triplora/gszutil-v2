/*
 * Copyright 2019 Google LLC All Rights Reserved.
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

class RandomBytes(val size: Long) extends ReadableByteChannel{
  private var consumed: Long = 0

  override def read(dst: ByteBuffer): Int = {
    if (consumed >= size) return -1
    var i = 111111L
    while (dst.remaining >= 32) {
      dst.putLong(i)
      dst.putLong(i)
      dst.putLong(i)
      dst.putLong(i)
      i += 1
    }
    while (dst.hasRemaining)
      dst.putChar('x')
    consumed += dst.position
    dst.position
  }

  private var open = true
  override def close(): Unit = open = false
  override def isOpen: Boolean = open
}
