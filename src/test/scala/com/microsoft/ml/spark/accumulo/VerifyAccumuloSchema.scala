// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import java.io.ByteArrayOutputStream

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class VerifyAccumuloSchema extends TestBase {
  test("Validate catalyst schema to json serialization") {
    val schema = (new StructType)
       .add(StructField("cf1", (new StructType)
          .add("cq1", DataTypes.StringType, true)
          .add("cq2", DataTypes.DoubleType, true)
          , true))
      .add(StructField("cf2", (new StructType)
        .add("cq_a", DataTypes.IntegerType, true)
        .add("cq_b", DataTypes.FloatType, true)
        , true))
      .add("rowKey", DataTypes.StringType, false)

    val jsonActual = AvroUtils.catalystSchemaToJson(schema)
    val jsonExpected = "[{\"cf\":\"cf1\",\"cq\":\"cq1\",\"t\":\"STRING\"}" +
      ",{\"cf\":\"cf1\",\"cq\":\"cq2\",\"t\":\"DOUBLE\"}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_a\",\"t\":\"INTEGER\"}" +
      ",{\"cf\":\"cf2\",\"cq\":\"cq_b\",\"t\":\"FLOAT\"}]"

    println(jsonActual)
    assert(jsonActual.equals(jsonExpected))
  }

  test("Validate catalyst schema to avro serialization") {
    val schema = (new StructType)
      .add(StructField("cf1", (new StructType)
        .add("cq1", DataTypes.StringType, true)
        .add("cq2", DataTypes.DoubleType, false)
        .add("cq3", DataTypes.DoubleType, true)
        , true))
      .add(StructField("cf2", (new StructType)
        .add("cq_a", DataTypes.IntegerType, true)
        .add("cq_b", DataTypes.FloatType, true)
        , true))
      .add("rowKey", DataTypes.StringType, false)

    val avroSchema = AvroUtils.catalystSchemaToAvroSchema(schema)

    val builder = new GenericRecordBuilder(avroSchema)

    val builderCf1 = new GenericRecordBuilder(avroSchema.getField("cf1").schema())
    val builderCf2 = new GenericRecordBuilder(avroSchema.getField("cf2").schema())
    // check if clear() helps perf?

    builderCf1.set("cq1", "foo")
    builderCf1.set("cq2", 2.3)

    builderCf2.set("cq_a", 1)
    builderCf2.set("cq_b", 1.2f)

    builder.set("cf1", builderCf1.build())
    builder.set("cf2", builderCf2.build())

    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.jsonEncoder(avroSchema, output)

    val writer = new SpecificDatumWriter[GenericRecord](avroSchema)
    writer.write(builder.build(), encoder)

    encoder.flush()

    val jsonOutput = new String(output.toByteArray)
    println(jsonOutput)

    val jsonExpected = "{\"cf1\":{\"cq1\":{\"string\":\"foo\"}," +
      "\"cq2\":2.3,\"cq3\":null}," +
      "\"cf2\":{\"cq_a\":{\"int\":1},\"cq_b\":{\"float\":1.2}}}"
    assert(jsonOutput.equals(jsonExpected))
  }
}
