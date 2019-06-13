package com.google.cloud.pso

import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.gszutil.Util.Logging
import com.google.cloud.gszutil.io.ZReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.{OrcConf, OrcFile}

object SimpleORCWriter extends Logging {
  def run(prefix: String,
          in: ReadableByteChannel,
          lRecl: Int,
          copyBook: CopyBook,
          writerOptions: WriterOptions,
          maxWriters: Int,
          batchSize: Int = 10000,
          partLen: Long = 1 * 1024 * 1024,
          timeoutMinutes: Int = 30): Unit = {
    var partId = 0
    var partSize = 0
    val uri = new URI(prefix)
    val reader = new ZReader(copyBook, batchSize)

    while (in.isOpen) {
      val partName = f"$partId%05d"
      val path = new Path(s"gs://${uri.getAuthority}/${uri.getPath.stripPrefix("/") + s"_$partName.orc"}")

      // Begin a new part
      val writer = OrcFile.createWriter(path, writerOptions)
      partSize = 0

      // Write part up to partLen records
      while (partSize < partLen && in.isOpen) {
        val bufSize = lRecl * batchSize
        val bb = ByteBuffer.allocate(bufSize)

        // fill buffer
        while (bb.hasRemaining && in.isOpen) {
          if (in.read(bb) < 0) {
            in.close()
          }
        }

        reader.readOrc(bb, writer, batchSize)
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
