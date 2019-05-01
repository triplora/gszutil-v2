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

import java.io.ByteArrayOutputStream

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory

import scala.util.Random


object AvroWriter {
  val AvroContentType = "avro/binary"

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
}
