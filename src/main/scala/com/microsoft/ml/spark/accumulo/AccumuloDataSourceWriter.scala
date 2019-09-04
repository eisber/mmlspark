// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class AccumuloDataSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new AccumuloDataWriterFactory(schema, mode, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }
}

class AccumuloDataWriterFactory(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new AccumuloDataWriter(schema, mode, options)
  }
}
