package com.google.cloud.pso

import java.net.URI
import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.{ZDataSet, ZRecordReaderT}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.{OrcConf, OrcFile}

object SimpleORCWriter extends Logging {
  def run(prefix: String,
          in: ZRecordReaderT,
          copyBook: CopyBook,
          writerOptions: WriterOptions,
          maxWriters: Int,
          batchSize: Int = 1024,
          partLen: Long = 1 * 1024 * 1024,
          timeoutMinutes: Int = 30): Unit = {
    var partId = 0
    var partSize = 0
    val uri = new URI(prefix)

    while (in.isOpen) {
      val partName = f"$partId%05d"
      val path = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/") + s"_$partName"}")

      // Begin a new part
      val writer = OrcFile.createWriter(path, writerOptions)
      partSize = 0

      // Write part up to partLen records
      while (partSize < partLen && in.isOpen) {
        val bufSize = in.lRecl * batchSize
        val bb = ByteBuffer.allocate(bufSize)
        val buf = bb.array()

        // fill buffer
        while (bb.hasRemaining && in.isOpen) {
          val n = in.read(buf, bb.position, bb.remaining)
          if (n < 0) {
            bb.limit(bb.position)
            in.close()
          } else {
            val newPosition = bb.position + n
            bb.position(newPosition)
          }
        }

        val reader = copyBook.reader
        reader
          .readOrc(new ZDataSet(buf, in.lRecl, in.blkSize, bb.position))
          .filter(_.size > 0)
          .foreach{rowBatch =>
            partSize += rowBatch.size
            writer.addRowBatch(rowBatch)
          }
      }
      writer.close()
      partId += 1
    }
  }


  def configuration(c: Configuration = new Configuration(false)): Configuration = {
    OrcConf.COMPRESS.setString(c, "none")
    OrcConf.ENABLE_INDEXES.setBoolean(c, false)
    OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
    OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
    c
  }
}
