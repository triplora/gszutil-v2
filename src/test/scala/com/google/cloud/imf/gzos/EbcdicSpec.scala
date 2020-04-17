package com.google.cloud.imf.gzos

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.scalatest.flatspec.AnyFlatSpec
import sun.nio.cs.ext.IBM1047

class EbcdicSpec extends AnyFlatSpec {
  "EBCDIC" should "write" in {
    val cs = new IBM1047()
    val bytes = (0 until 256).map(_.toByte).toArray
    val charBuf = cs.newDecoder().decode(ByteBuffer.wrap(bytes))
    val chars = charBuf.array()
    chars.update(186,'[')
    chars.update(187,']')
    val bytes1 = cs.newEncoder().encode(charBuf).array()
    bytes1.update(186,91)
    bytes1.update(187,93)
    Files.write(Paths.get("src/main/resources/ebcdic.dat"), bytes1,
      StandardOpenOption.CREATE,StandardOpenOption.WRITE)

    val bytesUtf8 = new String(chars).getBytes(StandardCharsets.UTF_8)
    Files.write(Paths.get("src/main/resources/ebcdic.txt"), bytesUtf8,
      StandardOpenOption.CREATE,StandardOpenOption.WRITE)
  }

  it should "match IBM1047" in {
    val ebcdic = new EBCDIC1
    val ibm1047 = new IBM1047
    val bytes = (0 until 256).map(_.toByte).toArray
    val actual = ebcdic.decode(ByteBuffer.wrap(bytes)).toString.toCharArray.toIndexedSeq
    val expected = ibm1047.decode(ByteBuffer.wrap(bytes)).toString.toCharArray
    expected.update(186,'[')
    expected.update(187,']')
    val expected1 = expected.toIndexedSeq
    assert(actual == expected1)
  }
}
