package com.google.cloud.gszutil.io

import com.google.cloud.bigquery.FieldValue
import com.google.common.io.BaseEncoding
import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class AvroUtilSpec extends AnyFlatSpec {

  it should "create FieldValue of type 'string'" in {
    val fieldSchema = new Schema.Field("a", Schema.create(Schema.Type.STRING), "", null)
    val fieldValue = new org.apache.avro.util.Utf8("abcd")
    val value = AvroUtil.toFieldValue(AvroField(fieldSchema), fieldValue)
    assert(!value.isNull)
    assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(value.getValue.isInstanceOf[String])
    assert(value.getStringValue == "abcd")
    //nulls
    val nullValue = AvroUtil.toFieldValue(AvroField(fieldSchema), null)
    assert(nullValue.isNull)
    assert(nullValue.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(nullValue.getValue == null)
  }

  it should "fail to create FieldValue of type 'string'" in {
    val schema = Schema.create(Schema.Type.STRING)
    schema.addProp("sqlType", "DATETIME")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    val fieldValue = new org.apache.avro.util.Utf8("2021-08-11")
    assertThrows[IllegalStateException] {
      AvroUtil.toFieldValue(AvroField(fieldSchema), fieldValue)
    }
  }

  it should "create FieldValue of type 'int64'" in {
    val fieldSchema = new Schema.Field("a", Schema.create(Schema.Type.LONG), "", null)
    val value = AvroUtil.toFieldValue(AvroField(fieldSchema), 123L)

    assert(!value.isNull)
    assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(value.getValue.isInstanceOf[String])
    assert(value.getValue.asInstanceOf[String] == "123")
    //nulls
    val nullValue = AvroUtil.toFieldValue(AvroField(fieldSchema), null)
    assert(nullValue.isNull)
    assert(nullValue.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(nullValue.getValue == null)
  }

  it should "fail to create FieldValue of type 'int64'" in {
    val schema = Schema.create(Schema.Type.LONG)
    schema.addProp("logicalType", "timestamp-micros")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    assertThrows[IllegalStateException] {
      AvroUtil.toFieldValue(AvroField(fieldSchema), 123456789L)
    }
  }

  it should "create FieldValue of type 'bytes'" in {
    val fieldSchema = new Schema.Field("a", Schema.create(Schema.Type.BYTES), "", null)
    val value = AvroUtil.toFieldValue(AvroField(fieldSchema), ByteBuffer.wrap("abcd".getBytes("utf-8")))

    assert(!value.isNull)
    assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(value.getValue.isInstanceOf[String])
    assert("abcd" == new String(BaseEncoding.base64.decode(value.getStringValue), "utf-8"))
    //nulls
    val nullValue = AvroUtil.toFieldValue(AvroField(fieldSchema), null)
    assert(nullValue.isNull)
    assert(nullValue.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(nullValue.getValue == null)
  }

  it should "fail to create FieldValue of type 'bytes'" in {
    val schema = Schema.create(Schema.Type.BYTES)
    schema.addProp("logicalType", "some-dummy-type")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    assertThrows[IllegalStateException] {
      AvroUtil.toFieldValue(AvroField(fieldSchema), ByteBuffer.wrap("abcd".getBytes("utf-8")))
    }
  }

  it should "create FieldValue of type 'date'" in {
    val schema = Schema.create(Schema.Type.INT)
    schema.addProp("logicalType", "date")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    val nowDate = LocalDate.now()
    val value = AvroUtil.toFieldValue(AvroField(fieldSchema), nowDate.toEpochDay.toInt)

    assert(!value.isNull)
    assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(value.getValue.isInstanceOf[String])
    assert(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(nowDate) == value.getStringValue)
    //nulls
    val nullValue = AvroUtil.toFieldValue(AvroField(fieldSchema), null)
    assert(nullValue.isNull)
    assert(nullValue.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(nullValue.getValue == null)
  }

  it should "fail to create FieldValue of type 'date'" in {
    val schema = Schema.create(Schema.Type.INT)
    schema.addProp("logicalType", "some-dummy-type")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    assertThrows[IllegalStateException] {
      AvroUtil.toFieldValue(AvroField(fieldSchema), 123123)
    }
  }

  it should "create FieldValue of type 'decimal'" in {
    val fieldSchema = new Schema.Field("a", Schema.create(Schema.Type.BYTES), "", null)
    fieldSchema.addProp("logicalType", "decimal")
    val value = AvroUtil.toFieldValue(AvroField(fieldSchema), ByteBuffer.wrap("123.123".getBytes("utf-8")))

    assert(!value.isNull)
    assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(value.getValue.isInstanceOf[String])
    assert("123.123" == new String(BaseEncoding.base64.decode(value.getStringValue), "utf-8"))
    //nulls
    val nullValue = AvroUtil.toFieldValue(AvroField(fieldSchema), null)
    assert(nullValue.isNull)
    assert(nullValue.getAttribute == FieldValue.Attribute.PRIMITIVE)
    assert(nullValue.getValue == null)
  }

  it should "fail to create FieldValue of type 'decimal'" in {
    val schema = Schema.create(Schema.Type.BYTES)
    schema.addProp("logicalType", "dummy")
    val fieldSchema = new Schema.Field("a", schema, "", null)
    assertThrows[IllegalStateException] {
      AvroUtil.toFieldValue(AvroField(fieldSchema), ByteBuffer.wrap("1".getBytes("utf-8")))
    }
  }
}
