package com.google.cloud.gszutil.io

import com.google.cloud.bigquery.FieldValue
import com.google.common.io.BaseEncoding
import org.apache.avro.Schema
import org.codehaus.jackson.node.IntNode
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
    val inputToExpected = List(
      ("0.100000000", "0.1"),
      ("999.999000000", "999.999"),
      ("1.000100000", "1.0001"),
      ("99999.999999000", "99999.999999")
    )

    val schema = Schema.create(Schema.Type.BYTES)
    schema.addProp("logicalType", "decimal")
    schema.addProp("scale", new IntNode(9))
    val fieldSchema = new Schema.Field("a", schema, "", null)

    inputToExpected.foreach {e =>
      val decBytes = new java.math.BigDecimal(e._1).unscaledValue.toByteArray
      val value = AvroUtil.toFieldValue(AvroField(fieldSchema), ByteBuffer.wrap(decBytes))

      assert(!value.isNull)
      assert(value.getAttribute == FieldValue.Attribute.PRIMITIVE)
      assert(value.getValue.isInstanceOf[String])
      assert(e._2 == value.getStringValue)
    }

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
