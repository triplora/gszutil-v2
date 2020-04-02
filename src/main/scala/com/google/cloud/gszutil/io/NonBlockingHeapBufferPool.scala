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

import akka.io.BufferPool

import scala.collection.mutable

class NonBlockingHeapBufferPool(bufSize: Int, poolCapacity: Int) extends BufferPool{
  private val queue: mutable.Queue[ByteBuffer] = {
    val q: mutable.Queue[ByteBuffer] = mutable.Queue.empty
    for (_ <- 1 to poolCapacity)
      q.enqueue(ByteBuffer.allocate(bufSize))
    q
  }

  override def acquire(): ByteBuffer =
    if (queue.nonEmpty) queue.dequeue() else ByteBuffer.allocate(bufSize)
  override def release(buf: ByteBuffer): Unit = queue.enqueue(buf)
}
