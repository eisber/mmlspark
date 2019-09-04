// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import java.io.ByteArrayOutputStream

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.clientImpl.{ClientContext, Tables, TabletServerBatchWriter}
import org.apache.accumulo.core.data.Mutation
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class AccumuloDataWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataWriter[InternalRow] {

    val properties = new java.util.Properties()
    properties.putAll(options.asMap())

    val context = new ClientContext(properties)
    // TODO: construct BatchWriterConfig from properties if passed in
    val batchWriter = new TabletServerBatchWriter(context, new BatchWriterConfig)

    private val tableName = options.tableName.get
    private val tableId = Tables.getTableId(context, tableName)

//    private val avroSchema = AvroUtils.catalystSchemaToAvroSchema(schema)
    private val json = AvroUtils.catalystSchemaToJson(schema)
    private val avroSchema = new Schema.Parser().parse(json)

    private val datumWriter = new SpecificDatumWriter[GenericRecord](avroSchema)
    private val binaryBuffer = new ByteArrayOutputStream
    private var encoder: BinaryEncoder = _

    def write(record: InternalRow): Unit = {
        encoder = EncoderFactory.get.binaryEncoder(binaryBuffer, encoder)

        val data = new GenericData.Record(avroSchema)
        // TODO: put values from record into data

        // avro -> byte[]
        datumWriter.write(data, encoder)
        encoder.flush()
        binaryBuffer.flush()

        val mutation = new Mutation(binaryBuffer.toByteArray)
        batchWriter.addMutation(tableId, mutation)
    }

    def commit(): WriterCommitMessage = {
        batchWriter.flush()
        batchWriter.close()

        WriteSucceeded
    }

    def abort(): Unit = {
        batchWriter.close()
    }

    object WriteSucceeded extends WriterCommitMessage
}
