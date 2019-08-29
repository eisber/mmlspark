// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import java.io.IOException

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.codehaus.jackson.map.ObjectMapper
import java.util.Collections

import scala.beans.BeanProperty

// keeping the property names short to not hit any limits
case class SchemaMappingField(
     @BeanProperty val cf: String,
     @BeanProperty val cq: String,
     @BeanProperty val t: String)

@SerialVersionUID(1L)
object AccumuloInputPartitionReader {
  def catalystSchemaToJson(schema: StructType): String = {

    val selectedFields = schema.fields.flatMap(cf =>
        cf.dataType match {
          case cft: StructType => cft.fields.map(cq => SchemaMappingField(
              cf.name,
              cq.name,
              // TODO: toUpperCase() is weird...
              cq.dataType.typeName.toUpperCase
            )
          )
            // Skip unknown types (e.g. string for row key)
          case other => None
          // case other => throw new IllegalArgumentException(s"Unsupported type: ${other}")
        }
    )
      .filter(p => p != None)

    try
      new ObjectMapper().writeValueAsString(selectedFields)
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  implicit class CatalystSchemaToAvroRecordBuilder(builder: SchemaBuilder.FieldAssembler[Schema]) {
    def addAvroRecordFields(schema: StructType): SchemaBuilder.FieldAssembler[Schema] = {
      schema.fields.foldLeft(builder) { (builder, field) =>
        field.dataType match {
          case DataTypes.StringType =>
            if (field.nullable)
              builder.optionalString(field.name)
            else
              builder.requiredString(field.name)

          case DataTypes.IntegerType =>
            if (field.nullable)
              builder.optionalInt(field.name)
            else
              builder.requiredInt(field.name)

          case DataTypes.FloatType =>
            if (field.nullable)
              builder.optionalFloat(field.name)
            else
              builder.requiredFloat(field.name)

          case DataTypes.DoubleType =>
            if (field.nullable)
              builder.optionalDouble(field.name)
            else
              builder.requiredDouble(field.name)

          case DataTypes.BooleanType =>
            if (field.nullable)
              builder.optionalBoolean(field.name)
            else
              builder.requiredBoolean(field.name)

          case DataTypes.LongType =>
            if (field.nullable)
              builder.optionalLong(field.name)
            else
              builder.requiredLong(field.name)

          case DataTypes.BinaryType =>
            if (field.nullable)
              builder.optionalBytes(field.name)
            else
              builder.requiredBytes(field.name)

          // TODO: date/time support?
          case other => throw new UnsupportedOperationException(s"Unsupported type: $field.dataType")
        }
      }
    }
  }

  def catalystSchemaToAvroSchema(schema: StructType): Schema = {
    val fieldBuilder = SchemaBuilder.record("root")
        .fields()

    schema.fields.foldLeft(fieldBuilder) {  (builder, field) =>
      field.dataType match {
        case cft: StructType =>
          // builder.record(field.name).fields().nullableBoolean().endRecord()
          fieldBuilder
              .name(field.name)
            .`type`(SchemaBuilder
                .record(field.name)
                .fields
                  .addAvroRecordFields(cft)
                .endRecord())
            .noDefault()

        case other => builder // just skip top-level non-struct fields
      }
    }
      .endRecord()
  }
}

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(val tableName: String,
                                   val properties: java.util.Properties,
                                   val schema: StructType)
  extends InputPartitionReader[InternalRow] with Serializable {

  // TODO: pull this from properties?
  final val priority = 20

  private val authorizations = new Authorizations()
  // private val client = new ClientContext(properties)

  // TODO: understand the relationship between client and clientContext
  private val client = Accumulo.newClient().from(properties).build()
  // private val tableId = Tables.getTableId(client, tableName)
  // private val scanner = new ScannerImpl(client, tableId, authorizations)
  // TODO: numThreads
  private val scanner = client.createBatchScanner(tableName, authorizations, 1)

  private val avroIterator = new IteratorSetting(
    priority,
    "AVRO",
    "org.apache.accumulo.spark.AvroRowEncoderIterator")

  private val json = AccumuloInputPartitionReader.catalystSchemaToJson(schema)

  // TODO: support additional user-supplied iterators
  avroIterator.addOption("schema", json)
  scanner.addScanIterator(avroIterator)

  // TODO: this needs to be determined by splits
  // See https://github.com/apache/accumulo/blob/master/core/src/main/java/org/apache/accumulo/core/client/BatchScanner.java
  scanner.setRanges(Collections.singletonList(new Range()));
  private val scannerIterator = scanner.iterator()

  private val avroSchema = AccumuloInputPartitionReader.catalystSchemaToAvroSchema(schema)
  private val deserializer = new AvroDeserializer(avroSchema, schema)
  private val reader = new SpecificDatumReader[GenericRecord](avroSchema)
  var row: InternalRow = _

  override def close(): Unit = {
    if (scanner != null)
      scanner.close()
  }

  @IOException
  override def next: Boolean = {
    if (scannerIterator.hasNext) {
      val entry = scannerIterator.next
      val data = entry.getValue.get

      // byte[] -> avro
      val decoder = DecoderFactory.get.binaryDecoder(data, null)
      val avroRecord = new GenericData.Record(avroSchema)
      reader.read(avroRecord, decoder)

      // avro to catalyst
      row = deserializer.deserialize(avroRecord).asInstanceOf[InternalRow]
      // TODO: pass row key
      // x: InternalRow
      // x.update(FieldIndex..
      // key.set(currentKey = entry.getKey());

      true
    } else {
      false
    }
  }

  override def get(): InternalRow = row
}
