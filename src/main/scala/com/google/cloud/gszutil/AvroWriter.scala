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

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}

import scala.util.Random


object AvroWriter {
  val AvroContentType = "avro/binary"
  val decimalType = LogicalTypes.decimal(9,2)
  val toDecimal = new Conversions.DecimalConversion()
  val bytesSchema = Schema.create(Schema.Type.BYTES)
  def convertDecimal(x: BigDecimal): ByteBuffer =
    toDecimal.toBytes(x.bigDecimal, null, decimalType)

  def buildSchema(): Schema = {
    val dateType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))

    SchemaBuilder
      .record("Example")
      .namespace("com.google.cloud.example")
      .fields()
        .name("date")
          .`type`(dateType).noDefault()
        .name("id")
          .`type`(Schema.Type.LONG.getName).noDefault()
        .name("description")
          .`type`(Schema.Type.STRING.getName).noDefault()
        .name("value")
          .`type`(Schema.Type.DOUBLE.getName).noDefault()
      .endRecord()
  }

  def create(n: Int): Array[Byte] = {
    val schema = buildSchema()
    val r = new Random()
    val w = new GenericDatumWriter[GenericRecord](schema)
    val outStream = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(outStream, null)

    for (_ <- 0 until n) {
      val record = new GenericData.Record(schema)
      record.put("date", System.currentTimeMillis() * 1000L)
      record.put("id", r.nextLong())
      record.put("description", r.nextString(10))
      record.put("value", r.nextDouble())
      w.write(record, encoder)
    }
    outStream.toByteArray
  }

  class ByteBufferOutputStream(private val buf: ByteBuffer) extends OutputStream {
    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      buf.put(b, off, len)
    }

    override def write(b: Int): Unit =
      buf.put(b.toByte)
  }

  class AvroPipe(records: Iterator[GenericData.Record], w: DatumWriter[GenericRecord], size: Int) extends InputStream {
    private val buf = ByteBuffer.allocate(size)
    private val os = new ByteBufferOutputStream(buf)
    private val encoder = EncoderFactory.get().directBinaryEncoder(os, null)

    override def read(): Int = throw new NotImplementedError()

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      // fill the buffer
      while (buf.remaining > size / 2 && records.hasNext) {
        w.write(records.next(), encoder)
      }
      encoder.flush()
      // prepare to read
      buf.compact()

      val n = math.min(len, buf.remaining)
      if (n > 0) {
        // read from buffer
        buf.get(b, off, n)
        // prepare to write
        buf.flip()
        n
      } else {
        if (records.hasNext) n
        else -1
      }
    }
  }

}
