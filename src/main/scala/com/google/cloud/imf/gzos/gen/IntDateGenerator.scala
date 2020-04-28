package com.google.cloud.imf.gzos.gen

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}

import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.io.Bytes
import com.google.cloud.imf.util.Bits

/** generates integer dates
  *
  * @param f
  */
class IntDateGenerator(f: Record.Field) extends ValueGenerator {
  override val size: Int = f.getSize
  override def toString: String = s"IntDateGenerator($size)"
  private val pattern = f.getFormat.replaceAllLiterally("D","d").replaceAllLiterally("Y","y")
  require(pattern.length == size,
    s"pattern length $pattern does not match field size $size")
  private val startDate = LocalDate.now(ZoneId.of("Etc/UTC")).minusDays(30)
  private var i = 0

  override def generate(buf: Array[Byte], off: Int): Int = {
    val genDate = startDate.plusDays(i)
    val genInt = ((((genDate.getYear - 1900) * 100) +
      genDate.getMonthValue) * 100) +
      genDate.getDayOfMonth
    val bytes = Bits.encodeIntL(genInt)
    assert(bytes.length == size)
    i += 1
    System.arraycopy(bytes, 0, buf, off, size)
    size
  }
}

