package com.google.cloud.gszutil.orc

import java.net.URI
import java.nio.channels.ReadableByteChannel

import com.google.cloud.gszutil.CopyBook
import com.google.cloud.storage.Storage

/**
  *
  * @param in input Data Set
  * @param batchSize rows per batch
  * @param uri prefix URI
  * @param maxBytes input bytes per part
  * @param nWorkers worker count
  * @param copyBook CopyBook
  */
case class DatasetReaderArgs(in: ReadableByteChannel, batchSize: Int, uri: URI, maxBytes: Long, nWorkers: Int, copyBook: CopyBook, gcs: Storage, compress: Boolean)
