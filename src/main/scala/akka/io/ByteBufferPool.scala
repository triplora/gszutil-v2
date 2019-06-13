package akka.io

object ByteBufferPool {
  def allocate(bufSize: Int, poolCapacity: Int): BufferPool = new DirectByteBufferPool(bufSize, poolCapacity)
}
