/*
 * Copyright 2019 Google LLC All Rights Reserved.
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

import com.google.cloud.gszutil.Decoding.{CopyBookField, CopyBookLine, Decoder, parseCopyBookLine}
import com.google.cloud.gzos.pb.Schema.Record

import scala.collection.mutable.ArrayBuffer


case class CopyBook(raw: String) extends SchemaProvider {
  final val Fields: Seq[CopyBookLine] = raw.lines.flatMap(parseCopyBookLine).toSeq

  override def fieldNames: Seq[String] =
    Fields.flatMap{
      case CopyBookField(name, _) =>
        Option(name.replaceAllLiterally("-","_"))
      case _ =>
        None
    }

  override def decoders: Array[Decoder] = {
    val buf = ArrayBuffer.empty[Decoder]
    Fields.foreach{
      case CopyBookField(_, decoder) =>
        buf.append(decoder)
      case _ =>
    }
    buf.toArray
  }

  override def toString: String =
    s"LRECL=$LRECL\nFIELDS=${fieldNames.mkString(",")}\n$raw\n\nORC TypeDescription:\n${ORCSchema.toJson}"

  override def toByteArray: Array[Byte] =
    toRecordBuilder.build().toByteArray

  def toRecordBuilder: Record.Builder = {
    val b = Record.newBuilder()
        .setLrecl(LRECL)
        .setSource(Record.Source.COPYBOOK)
        .setOriginal(raw)

    Fields.foreach{
      case CopyBookField(name, decoder) =>
        b.addField(decoder.toFieldBuilder.setName(name))
      case _ =>
    }

    b
  }
}

