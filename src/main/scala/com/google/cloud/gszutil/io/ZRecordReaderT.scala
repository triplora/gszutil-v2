package com.google.cloud.gszutil.io

trait ZRecordReaderT {

  /** Read a record from the dataset into a buffer.
    *
    * @param buf - the byte array into which the bytes will be read
    * @return the number of bytes read, -1 if EOF encountered.
    */
  def read(buf: Array[Byte]): Int

  /** Read a record from the dataset into a buffer.
    *
    * @param buf the byte array into which the bytes will be read
    * @param off the offset, inclusive in buf to start reading bytes
    * @param len the number of bytes to read
    * @return the number of bytes read, -1 if EOF encountered.
    */
  def read(buf: Array[Byte], off: Int, len: Int): Int

  /** Close the reader and underlying native file.
    * This will also free the associated DD if getAutoFree() is true.
    * Must be issued by the same thread as the factory method.
    */
  def close(): Unit

  def isOpen: Boolean

  /** LRECL is the maximum record length for variable length files.
    *
    * @return
    */
  val lRecl: Int

  /** Maximum block size
    *
    * @return
    */
  val blkSize: Int
}
