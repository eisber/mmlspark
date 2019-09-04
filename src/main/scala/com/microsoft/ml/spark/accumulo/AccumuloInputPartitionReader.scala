// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.io.Text
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import java.io.IOException
import java.util.Collections

import org.apache.avro.Schema

@SerialVersionUID(1L)
class AccumuloInputPartitionReader(tableName: String,
                                   start: Text,
                                   stop: Text,
                                   schema: StructType,
                                   properties: java.util.Properties)
  extends InputPartitionReader[InternalRow] with Serializable {

  val defaultPriority = "20"
  val defaultNumQueryThreads = "4"

  val priority = new Integer(properties.getProperty("priority", defaultPriority))
  val numQueryThreads = new Integer(properties.getProperty("numQueryThreads", defaultNumQueryThreads))

  private val authorizations = new Authorizations()

  // TODO: understand the relationship between client and clientContext
  // private val client = new ClientContext(properties)
  // private val tableId = Tables.getTableId(client, tableName)
  // private val scanner = new ScannerImpl(client, tableId, authorizations)

  private val client = Accumulo.newClient().from(properties).build()
  private val scanner = client.createBatchScanner(tableName, authorizations, numQueryThreads)
  scanner.setRanges(Collections.singletonList(new Range(start, false, stop, true)))

  private val avroIterator = new IteratorSetting(
    priority,
    "AVRO",
    "org.apache.accumulo.spark.AvroRowEncoderIterator")

  private val json = AvroUtils.catalystSchemaToJson(schema)

  // TODO: support additional user-supplied iterators
  avroIterator.addOption("schema", json)
  scanner.addScanIterator(avroIterator)

  private val scannerIterator = scanner.iterator()

  //private val avroSchema = AvroUtils.catalystSchemaToAvroSchema(schema)
  private val avroSchema = new Schema.Parser().parse(json)
  private val deserializer = new AvroDeserializer(avroSchema, schema)
  private val reader = new SpecificDatumReader[GenericRecord](avroSchema)

  private var decoder: BinaryDecoder = _
  private var currentRow: InternalRow = _
  private var datum: GenericRecord = _

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
      decoder = DecoderFactory.get.binaryDecoder(data, decoder)
      datum = reader.read(datum, decoder)

      // avro to catalyst
      currentRow = deserializer.deserialize(datum).asInstanceOf[InternalRow]
      // TODO: pass row key
      // x: InternalRow
      // x.update(FieldIndex..
      // key.set(currentKey = entry.getKey());

      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow
}
