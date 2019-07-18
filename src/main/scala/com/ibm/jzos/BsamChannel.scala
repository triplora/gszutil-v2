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

package com.ibm.jzos

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

import com.google.cloud.gszutil.io.ZRecordReaderT

class BsamChannel(dd: String) extends ZRecordReaderT with ReadableByteChannel {

  private val bsam = new Bsam(dd, Bsam.OPEN_INPUT)
  require(bsam.getRecfm == "FB", "RECFM must be FB")
  require(bsam.getBlksize % bsam.getLrecl == 0, "BLKSIZE is not a multiple of LRECL")

  def free(): Unit = {
    try {
      ZFile.bpxwdyn(s"free fi($dd) msg(2)")
    } catch {
      case e: RcException =>
        ZUtil.logDiagnostic(1, s"RecordReader: Error freeing DD:$dd - ${e.getMessage}")
    }
  }

  /** Read a record from the dataset into a buffer.
    *
    * @param buf - the byte array into which the bytes will be read
    * @return the number of bytes read, -1 if EOF encountered.
    */
  override def read(buf: Array[Byte]): Int =
    bsam.readBlock(buf)

  /** Read a record from the dataset into a buffer.
    *
    * @param buf the byte array into which the bytes will be read
    * @param off the offset, inclusive in buf to start reading bytes
    * @param len the number of bytes to read
    * @return the number of bytes read, -1 if EOF encountered.
    */
  override def read(buf: Array[Byte], off: Int, len: Int): Int =
    bsam.readBlock(buf, off, len)

  /** Close the reader and underlying native file.
    * This will also free the associated DD if getAutoFree() is true.
    * Must be issued by the same thread as the factory method.
    */
  override def close(): Unit = bsam.close()

  override def isOpen: Boolean = bsam.isOpen

  /** LRECL is the maximum record length for variable length files.
    *
    * @return
    */
  override val lRecl: Int = bsam.getLrecl

  /** Maximum block size
    *
    * @return
    */
  override val blkSize: Int = bsam.getBlksize

  override def read(dst: ByteBuffer): Int = {
    val i = dst.position
    val remaining = dst.remaining
    if (remaining >= blkSize) {
      val n = read(dst.array(), i, remaining)
      if (n > 0) dst.position(i + n)
      n
    } else {
      throw new RuntimeException(s"insufficient space in buffer (remaining=$remaining but BLKSIZE=$blkSize)")
    }
  }
}
