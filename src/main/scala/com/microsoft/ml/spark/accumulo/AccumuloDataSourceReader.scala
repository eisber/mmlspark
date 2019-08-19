// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.clientImpl.ClientContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

@SerialVersionUID(1L)
class AccumuloDataSourceReader(val schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable {

  // TODO: get this from somewhere?
  val maxPartitions = 100

  def readSchema: StructType = schema

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val props = options.asMap()
    val properties = new java.util.Properties()
    properties.putAll(props)

    // match partitions to accumulo tabletservers for given table
    val client = new ClientContext(properties)
    val numTablets = client.tableOperations().listSplits(tableName, maxPartitions).size() + 1

    val partitionReaderFactories = new java.util.ArrayList[InputPartition[InternalRow]]
    for (_ <- 0 until numTablets) {
      partitionReaderFactories.add(new PartitionReaderFactory(tableName, properties, schema))
    }
    partitionReaderFactories
  }
}

class PartitionReaderFactory(tableName: String, properties: java.util.Properties, schema: StructType)
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, properties, schema)
  }
}
