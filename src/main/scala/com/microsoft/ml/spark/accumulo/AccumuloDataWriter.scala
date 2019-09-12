// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{StructField, StructType}

class AccumuloDataWriter (tableName: String, schema: StructType, mode: SaveMode, properties: java.util.Properties)
  extends DataWriter[InternalRow] {

    // val context = new ClientContext(properties)
    // TODO: construct BatchWriterConfig from properties if passed in
    // val batchWriter = new TabletServerBatchWriter(context, new BatchWriterConfig)
    // private val tableId = Tables.getTableId(context, tableName)

    private val client = Accumulo.newClient().from(properties).build();
    private val batchWriter = client.createBatchWriter(tableName)

    def write(record: InternalRow): Unit = {
        schema.fields.zipWithIndex.foreach {
            case (cf: StructField, structIdx: Int) =>
                cf.dataType match {
                    case struct: StructType =>
                        struct.fields.zipWithIndex.foreach {
                            case (cq: StructField, fieldIdx: Int) =>
                                val recordStruct = record.getStruct(structIdx, struct.size)
                                // FIXME: put in correct row id
                                batchWriter.addMutation(new Mutation(new Text("row_id"))
                                  .at()
                                  .family(cf.name)
                                  .qualifier(cq.name)
                                  .put(new Text(recordStruct.getString(fieldIdx)))
                                )
                        }
                }
        }
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
