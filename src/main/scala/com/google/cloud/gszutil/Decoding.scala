/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gszutil

import java.nio.{ByteBuffer, CharBuffer}


object Decoding {
  def uint(b: Byte): Int = {
    if (b < 0) 256 + b
    else b
  }

  def pad(x: Int): String = {
    val s = x.toString
    val n = s.length
    if (n < 4)
      "    ".substring(0, 4 - n) + s
    else s
  }

  trait Decoder[T] {
    def apply(src: ByteBuffer): T
  }

  class CharDecoder extends Decoder[Char] {
    private val decoder = ZReader.CP1047.newDecoder()
    private val cb = CharBuffer.allocate(1)

    override def apply(src: ByteBuffer): Char = {
      cb.clear()
      decoder.decode(src, cb, true)
      cb.get(0)
    }
  }

  class ShortDecoder extends Decoder[Short] {
    private val bytes = new Array[Byte](2)

    override def apply(src: ByteBuffer): Short = {
      src.get(bytes)
      ((bytes(0) << 8) |
        (bytes(1) & 255)).toShort
    }
  }

  class IntDecoder extends Decoder[Int] {
    private val bytes = new Array[Byte](4)

    override def apply(src: ByteBuffer): Int = {
      src.get(bytes)
      (bytes(0) << 24) |
        ((bytes(1) & 255) << 16) |
        ((bytes(2) & 255) << 8) |
        (bytes(3) & 255)
    }
  }

  class FloatDecoder extends Decoder[BigDecimal] {
    private val intDecoder = new IntDecoder
    private val shortDecoder = new ShortDecoder

    override def apply(src: ByteBuffer): BigDecimal = {
      val s = intDecoder(src).toLong * 100
      val p = shortDecoder(src).toLong
      BigDecimal(s + p, 2)
    }
  }

  trait DataSet[T]{
    val LRECL: Int
    def read(array: Array[Byte]): T
    def read(buf: ByteBuffer): T
    def read(records: Iterator[Array[Byte]]): Iterator[T]
  }
}
