/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

case class CloudRecordReader(dsn: Seq[String],
                             override val lRecl: Int) extends ZRecordReaderT {
  override def read(dst: ByteBuffer): Int = -1
  override def read(buf: Array[Byte]): Int = -1
  override def read(buf: Array[Byte], off: Int, len: Int): Int = -1
  override def close(): Unit = {}
  override def isOpen: Boolean = false
  override def getDsn: String = dsn.headOption.getOrElse("")
  override def count(): Long = 0
  override val blkSize: Int = lRecl
}
