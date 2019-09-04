// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.Accumulo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import com.microsoft.ml.spark.core.env.StreamUtilities
import org.apache.hadoop.io.Text

import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class AccumuloDataSourceReader(schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable {

  private val defaultMaxPartitions = 1000

  def readSchema: StructType = schema

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val maxPartitions = options.getInt("maxPartitions", defaultMaxPartitions)
    val properties = new java.util.Properties()
    properties.putAll(options.asMap())

    // val client = new ClientContext(properties)
    // match partitions to accumulo tabletservers (numSplits + 1) for given table
    val splits = StreamUtilities.using(Accumulo.newClient().from(properties).build()) { client =>
      client.tableOperations().listSplits(tableName, maxPartitions)
    }.get.asScala.toArray

    var start: Text = null
    var stop: Text = null
    new java.util.ArrayList[InputPartition[InternalRow]](
      (0 to splits.length).map(i => {
        start = if (i > 0) splits(i - 1) else null
        stop = if (i < splits.length) splits(i) else null
        new PartitionReaderFactory(
          tableName,
          start,
          stop,
          schema,
          properties)
      }).asJava
    )
  }
}

class PartitionReaderFactory(tableName: String,
                             start: Text,
                             stop: Text,
                             schema: StructType,
                             properties: java.util.Properties)
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, start, stop, schema, properties)
  }
}
