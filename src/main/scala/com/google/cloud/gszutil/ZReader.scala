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

import com.ibm.jzos.{RecordReader, ZFile}

object ZReader {
  def readDD(ddName: String): InputStream = {
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    val reader = RecordReader.newReaderForDD(ddName)
    System.out.println(s"Reading DD $ddName ${reader.getDsn}")
    new RecordReaderInputStream(reader)
  }

  private class RecordReaderInputStream(in: RecordReader) extends InputStream {
    private val byte = new Array[Byte](1)
    private var bytesRead: Long = 0
    def getBytesRead: Long = bytesRead

    override def read(): Int = {
      in.read(byte)
      bytesRead += 1
      byte(0)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      val n = in.read(b, off, len)
      bytesRead += n
      n
    }

    override def close(): Unit = {
      System.out.println(s"Read $bytesRead bytes from RecordReader")
      super.close()
    }
  }
}
