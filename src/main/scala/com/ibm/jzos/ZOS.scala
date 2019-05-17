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
package com.ibm.jzos

import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZRecordReaderT

object ZOS extends Logging {
  class RecordReaderCloser(r: RecordReader) extends Thread { override def run(): Unit = r.close() }

  class WrappedRecordReader(r: RecordReader) extends ZRecordReaderT {
    // Ensure that reader is closed if job is killed
    //Runtime.getRuntime.addShutdownHook(new RecordReaderCloser(r))
    private var open = true

    override def read(buf: Array[Byte]): Int =
      r.read(buf)
    override def read(buf: Array[Byte], off: Int, len: Int): Int =
      r.read(buf, off, len)
    override def close(): Unit = {
      if (open) {
        open = false
        try {
          r.close() //fails if we are on BSAM
        } catch {
          case e: ZFileException =>
            val msg =
              s"""${e.getMessage}
                 |SynadMsg: ${e.getSynadMsg}
                 |AbendCode: ${e.getAbendCode}
                 |AbendRc: ${e.getAbendRc}
                 |AllocSvc99Error: ${e.getAllocSvc99Error}
                 |AllocSvc99Info: ${e.getAllocSvc99Info}
                 |Errno: ${e.getErrno}
                 |Errno2: ${e.getErrno2}
                 |ErrMsg: ${e.getErrnoMsg}
                 |ErrorCode: ${e.getErrorCode}
                 |Feedback: ${e.getFeedbackFdbk}
                 |FeedbackFtncd: ${e.getFeedbackFtncd}
                 |FeedbackRc: ${e.getFeedbackRc}
                 |LastOp: ${e.getLastOp}
                 |""".stripMargin
            logger.error(msg)
            e.printStackTrace(System.err)
        }
      }
    }

    override def isOpen: Boolean = open
    override val lRecl: Int = r.getLrecl
    override val blkSize: Int = r.getBlksize
  }

  def readDD(ddName: String, bsamFb: Boolean = false): ZRecordReaderT = {
    if (!ZFile.ddExists(ddName))
      throw new RuntimeException(s"DD $ddName does not exist")

    val reader: RecordReader = if (bsamFb){
      new BsamFbRecordReader(new Bsam(ddName, ZFileConstants.OPEN_MODE_BINARY))
    } else {
      RecordReader.newReaderForDD(ddName)
    }
    logger.info(s"Reading DD $ddName ${reader.getDsn} with record format ${reader.getRecfm} BLKSIZE ${reader.getBlksize} LRECL ${reader.getLrecl}")
    new WrappedRecordReader(reader)
  }
}
