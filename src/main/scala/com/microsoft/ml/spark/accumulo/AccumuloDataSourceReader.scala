// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.clientImpl.ClientContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class AccumuloDataSourceReader(val schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable {

  // TODO: get this from somewhere?
  val maxPartitions = 100

  def readSchema: StructType = schema

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val properties = new java.util.Properties()
    properties.putAll(options.asMap())

    // match partitions to accumulo tabletservers for given table
    val client = new ClientContext(properties)
    val numTablets = client.tableOperations().listSplits(tableName, maxPartitions).size()

    (0 to numTablets).map(_ => new PartitionReaderFactory(tableName, properties, schema)).asJava
  }
}

class PartitionReaderFactory(tableName: String, properties: java.util.Properties, schema: StructType)
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, properties, schema)
  }
}
