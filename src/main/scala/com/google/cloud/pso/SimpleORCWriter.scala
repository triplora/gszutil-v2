package com.google.cloud.pso

import java.net.URI
import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZDataSet, ZRecordReaderT}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.OrcFile.WriterOptions

object SimpleORCWriter extends Logging {
  def run(prefix: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          writerOptions: WriterOptions,
          maxWriters: Int,
          batchSize: Int = 1024,
          partLen: Long = 32 * 1024 * 1024,
          timeoutMinutes: Int = 30)= {
    var writerId = 0
    val uri = new URI(prefix)

    while (in.isOpen) {
      val bufSize = in.lRecl * batchSize
      val bb = ByteBuffer.allocate(bufSize)
      val buf = bb.array()
      while (bb.hasRemaining && in.isOpen){
        val n = in.read(buf, bb.position, bb.remaining)
        if (n < 0) {
          bb.limit(bb.position)
          in.close()
        } else {
          val newPosition = bb.position + n
          bb.position(newPosition)
        }
      }

      val name = f"$writerId%05d"
      val path = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/") + s"_$name"}")
      val reader = copyBook.reader
      val writer = OrcFile.createWriter(path, writerOptions)
      val rr = new ZDataSet(buf, in.lRecl, in.blkSize, bb.position)
      reader
        .readOrc(rr)
        .filter(_.size > 0)
        .foreach{rowBatch =>
          writer.addRowBatch(rowBatch)
        }

      writer.close()
      writerId += 1
    }
  }


  def configuration(c: Configuration = new Configuration(false)): Configuration = {
    OrcConf.COMPRESS.setString(c, "none")
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    c
  }
}
