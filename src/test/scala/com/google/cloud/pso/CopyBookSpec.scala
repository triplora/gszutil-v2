package com.google.cloud.pso

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.TestRecordReader
import com.google.cloud.gszutil.ZReader.ByteIterator
import com.google.common.io.Resources
import org.apache.commons.io.Charsets
import org.scalatest.FlatSpec

class CopyBookSpec extends FlatSpec {
  def read(x: String): String = {
    new String(Resources.toByteArray(Resources.getResource(x).toURI.toURL), Charsets.UTF_8)
  }

  def readB(x: String): Array[Byte] = {
    Resources.toByteArray(Resources.getResource(x).toURI.toURL)
  }

  "CopyBook" should "parse" in {
    for (name <- (1 to 3).map(i => s"test$i.cpy")){
      val cb1 = CopyBook(read(name))
      val s = cb1.lines.map(_.toString).mkString("\n")
      System.out.println("\n*********************************\n")
      System.out.println(s)
      System.out.println("\n*********************************\n")
    }
  }

  it should "read" in {
    val copyBook = CopyBook(read("test1.cpy"))
    val reader = copyBook.reader()
    val bytes = new ByteIterator(new TestRecordReader(readB("test.bin"), copyBook.lrecl))
    val rows = reader.read(bytes)
    System.out.println(rows.map(_.toString).mkString("\n"))
  }
}
