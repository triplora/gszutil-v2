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

package com.google.cloud.gszutil.orc

import java.net.URI
import java.nio.channels.ReadableByteChannel

import akka.actor.{ActorSystem, Props}
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.storage.Storage
import com.google.common.collect.ImmutableMap
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await

object WriteORCFile extends Logging {
  def run(gcsUri: String,
          in: ReadableByteChannel,
          copyBook: CopyBook,
          gcs: Storage,
          maxWriters: Int,
          batchSize: Int,
          partSizeMb: Long,
          timeoutMinutes: Int,
          compress: Boolean): Unit = {
    import scala.concurrent.duration._
    val conf = ConfigFactory.parseMap(ImmutableMap.of(
      "akka.actor.guardian-supervisor-strategy","akka.actor.EscalatingSupervisorStrategy"))
    val sys = ActorSystem("gsz", conf)
    val bufSize = copyBook.LRECL * batchSize
    //val pool = ByteBufferPool.allocate(bufSize, maxWriters)
    val pool = new NoOpHeapBufferPool(bufSize, maxWriters)
    val args = DatasetReaderArgs(
      in = in,
      batchSize = batchSize,
      uri = new URI(gcsUri),
      maxBytes = partSizeMb*1024*1024,
      nWorkers = maxWriters,
      copyBook = copyBook,
      gcs = gcs,
      compress = compress,
      pool = pool)
    sys.actorOf(Props(classOf[DatasetReader], args), "ZReader")
    Await.result(sys.whenTerminated, atMost = FiniteDuration(timeoutMinutes, MINUTES))
  }
}
