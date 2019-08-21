// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.clientImpl.{ClientContext, ScannerImpl, Tables}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataType, DataTypes, StringType, StructField, StructType}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.NullNode

import scala.beans.BeanProperty
import scala.collection.JavaConversions

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
//
//  def catalystSchemaToAvroSchema(schema: StructType, onlyStructs: Boolean = false): java.util.List[Schema.Field] = {
//    // compile-time method binding for Schema.create. yes it's deprecated. yes it's the only version
//    // available in the spark version deployed
//
//    val avroFields = schema.fields.map(field => {
//      val avroField = field.dataType match {
//        case cft: StructType =>
//          Schema.createRecord(catalystSchemaToAvroSchema(cft))
//        case other =>
//          Schema.create(field.dataType match {
//            case DataTypes.StringType => Schema.Type.STRING
//            case DataTypes.IntegerType => Schema.Type.INT
//            case DataTypes.FloatType => Schema.Type.FLOAT
//            case DataTypes.DoubleType => Schema.Type.DOUBLE
//            case DataTypes.BooleanType => Schema.Type.BOOLEAN
//            case DataTypes.LongType => Schema.Type.LONG
//            case other => throw new UnsupportedOperationException(s"Unsupported type: $field.dataType")
//          })
//      }
//
//      new Schema.Field(
//        field.name,
//        avroField,
//        null.asInstanceOf[String],
//        // all fields are nullable for now...
//        null,
//        Schema.Field.Order.IGNORE
//      )
//    })
//        .filter(f => !onlyStructs || f.schema().getType().equals(Schema.Type.RECORD))
//
//    JavaConversions.seqAsJavaList[Schema.Field](avroFields)
//  }
}

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(val tableName: String,
                                   val properties: java.util.Properties,
                                   val schema: StructType)
  extends InputPartitionReader[InternalRow] with Serializable {

  // TODO: pull this from properties?
  final val priority = 20

  private val authorizations = new Authorizations()
  private val client = new ClientContext(properties)
  private val tableId = Tables.getTableId(client, tableName)
  private val scanner = new ScannerImpl(client, tableId, authorizations)

  private val avroIterator = new IteratorSetting(
    priority,
    "AVRO",
    "org.apache.accumulo.spark.AvroRowEncoderIterator")

  private val json = AccumuloInputPartitionReader.catalystSchemaToJson(schema)

  // TODO: support additional user-supplied iterators
  avroIterator.addOption("schema", json)
  scanner.addScanIterator(avroIterator)

  // TODO: ?
  // scanner.setRange(baseSplit.getRange());
  private val scannerIterator = scanner.iterator()

  // private val avroSchema = Schema.createRecord(AccumuloInputPartitionReader.catalystSchemaToAvroSchema(schema, true))
  private val avroSchema = AccumuloInputPartitionReader.catalystSchemaToAvroSchema(schema)
  private val deserializer = new AvroDeserializer(avroSchema, schema)
  private val reader = new SpecificDatumReader[GenericRecord](avroSchema)

  def close(): Unit = {
    if (scanner != null)
      scanner.close()
  }

  def next: Boolean = scannerIterator.hasNext

  def get: InternalRow = {
    val entry = scannerIterator.next
    // TODO: handle key
    // key.set(currentKey = entry.getKey());

    val data = entry.getValue.get

    // byte[] -> avro
    val decoder = DecoderFactory.get.binaryDecoder(data, null)
    val avroRecord = new GenericData.Record(avroSchema)
    reader.read(avroRecord, decoder)

    // avro to catalyst
    deserializer.deserialize(avroRecord).asInstanceOf[InternalRow]
  }
}
