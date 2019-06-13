package com.google.cloud.gszutil.orc

import akka.io.BufferPool
import com.google.cloud.gszutil.CopyBook
import com.google.cloud.storage.Storage
import org.apache.hadoop.fs.Path

/**
  *
  * @param copyBook CopyBook
  * @param maxBytes number of bytes to accept before closing the writer
  * @param batchSize records per batch
  */
case class ORCFileWriterArgs(copyBook: CopyBook, maxBytes: Long, batchSize: Int, path: Path, gcs: Storage, compress: Boolean, pool: BufferPool)
