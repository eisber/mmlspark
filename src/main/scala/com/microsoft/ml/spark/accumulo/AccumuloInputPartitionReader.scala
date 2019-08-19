// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.clientImpl.{ClientContext, ScannerImpl, Tables}
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.codehaus.jackson.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConversions

case class SchemaMappingField(columnFamily: String, columnQualifier: String, typeName: String)

// TODO: remove rowKeyTargetColumn
case class SchemaMapping(rowKeyTargetColumn: String, mapping: Map[String, SchemaMappingField])

@SerialVersionUID(1L)
object AccumuloInputPartitionReader {
  def catalystSchemaToJson(schema: StructType): String = {

    val mappingFields = schema.fields.map(field => field.name -> SchemaMappingField(
      field.metadata.getString("cf"),
      field.metadata.getString("cq"),
      // TODO: toUpperCase() is weird...
      catalystToAvroType(field.dataType).getName.toUpperCase
    )).toMap

    try
      new ObjectMapper().writeValueAsString(SchemaMapping("", mappingFields))
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  def catalystSchemaToAvroSchema(schema: StructType): Schema = {
    // compile-time method binding. yes it's deprecated. yes it's the only version
    // available in the spark version deployed
    val avroFields = schema.fields.map(field =>
      new Schema.Field(
        field.name,
        Schema.create(catalystToAvroType(field.dataType)),
        null.asInstanceOf[String],
        null.asInstanceOf[JsonNode]))

    Schema.createRecord(JavaConversions.seqAsJavaList(avroFields))
  }

  def catalystToAvroType(dataType: DataType): Schema.Type =
    dataType match {
      case DataTypes.StringType => Schema.Type.STRING
      case DataTypes.IntegerType => Schema.Type.INT
      case DataTypes.FloatType => Schema.Type.FLOAT
      case DataTypes.DoubleType => Schema.Type.DOUBLE
      case DataTypes.BooleanType => Schema.Type.BOOLEAN
      case DataTypes.LongType => Schema.Type.LONG
      case _ => throw new UnsupportedOperationException(s"Unsupported type: $dataType")
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
