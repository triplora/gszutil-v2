package org.apache.orc

import org.apache.hadoop.fs.Path

object NoOpMemoryManager extends MemoryManager {
  override def addWriter(path: Path, requestedAllocation: Long, callback: MemoryManager.Callback): Unit = {}
  override def removeWriter(path: Path): Unit = {}
  override def addedRow(rows: Int): Unit = {}
}
