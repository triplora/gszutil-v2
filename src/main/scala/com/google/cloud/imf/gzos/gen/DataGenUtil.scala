package com.google.cloud.imf.gzos.gen

import java.nio.charset.Charset

import com.google.cloud.gszutil.SchemaProvider
import com.google.cloud.imf.gzos.Ebcdic
import com.google.cloud.imf.gzos.pb.GRecvProto.Record
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field.FieldType

object DataGenUtil {
  def generatorFor(sp: SchemaProvider): DataGenerator = {
    new DataGenerator(sp, 100, Ebcdic.charset)
  }

  def getGenerator(f: Record.Field, charset: Charset): ValueGenerator = {
    f.getTyp match {
      case FieldType.DECIMAL =>
        new DecimalGenerator(f)
      case FieldType.INTEGER =>
        new IntegerGenerator(f)
      case FieldType.STRING | FieldType.LATIN_STRING =>
        if (f.getCast == FieldType.DATE)
          new DateGenerator(f, charset)
        else
          new StringGenerator(f, charset)
      case x =>
        throw new RuntimeException(s"unable to generate data for $x")
    }
  }
}
