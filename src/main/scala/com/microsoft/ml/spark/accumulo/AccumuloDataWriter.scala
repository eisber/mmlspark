// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.clientImpl.{ClientContext, Tables, TabletServerBatchWriter}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
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

    def write(record: InternalRow): Unit = {

        var i = 0
        schema.fields.foreach(cf =>
            cf.dataType match {
                case cft: StructType => cft.fields.foreach(cq => {
                    // TODO: put in row id
                    val mutation = new Mutation()
                    mutation.put(
                        new Text(cf.name),
                        new Text(cq.name),
                        new Value(new Text(record.getString(i))))
                    batchWriter.addMutation(tableId, mutation)
                    i += 1
                })
            }
        )
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
