// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.Accumulo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import com.microsoft.ml.spark.core.env.StreamUtilities

import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class AccumuloDataSourceReader(val schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable {

  // TODO: get this from somewhere?
  val maxPartitions = 1000

  def readSchema: StructType = schema

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val properties = new java.util.Properties()
    properties.putAll(options.asMap())

    // val client = new ClientContext(properties)
    val numSplits = StreamUtilities.using(Accumulo.newClient().from(properties).build()) { client =>
	    client.tableOperations().listSplits(tableName, maxPartitions).size()
    }.get
      // match partitions to accumulo tabletservers (numSplits + 1) for given table
      new java.util.ArrayList[InputPartition[InternalRow]](
      (0 to numSplits)
        .map(_ => new PartitionReaderFactory(tableName, properties, schema))
        .asJava)
  }
}

class PartitionReaderFactory(tableName: String, properties: java.util.Properties, schema: StructType)
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, properties, schema)
  }
}
