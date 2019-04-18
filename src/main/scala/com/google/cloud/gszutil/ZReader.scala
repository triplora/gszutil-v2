/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.gszutil

import java.io.InputStream
import java.nio.ByteBuffer

import com.ibm.jzos.{RecordReader, ZFile}

object ZReader {
  def readDD(ddName: String): InputStream = {
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    val reader = RecordReader.newReaderForDD(ddName)
    reader.setAutoFree(true)
    System.out.println(s"Reading DD $ddName ${reader.getDsn} with record format ${reader.getRecfm} BLKSIZE ${reader.getBlksize} LRECL ${reader.getLrecl}")
    new RecordReaderInputStream(reader)
  }

  private class RecordReaderInputStream(in: RecordReader) extends InputStream {
    private val lrecl = in.getLrecl // maximum record length
    private val data = new Array[Byte](lrecl)
    private val buf = ByteBuffer.allocate(lrecl * 100)
    private var finished = false
    private var bytesRead: Long = 0
    private var bytesReturned: Long = 0

    def fill(): Unit = {
      while (buf.remaining >= lrecl) {
        val n = in.read(data)
        if (n < 1) finished = true
        else {
          bytesRead += n
          buf.put(data, 0, n)
        }
      }
    }

    def getBytesRead: Long = bytesRead

    override def read(): Int = {
      fill()
      buf.flip
      val byte = buf.get()
      buf.flip
      bytesReturned += 1
      byte
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      fill()
      buf.flip
      val nBytes = if (buf.remaining < len) buf.remaining else len
      buf.get(b, off, nBytes)
      buf.flip
      bytesReturned += nBytes
      nBytes
    }

    override def close(): Unit = {
      System.out.println(s"Read $bytesRead bytes from RecordReader and output $bytesReturned bytes")
      super.close()
    }
  }
}
