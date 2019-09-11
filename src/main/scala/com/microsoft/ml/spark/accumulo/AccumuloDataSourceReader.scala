// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import org.apache.accumulo.core.client.Accumulo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
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

    val client = Accumulo.newClient().from(properties).build()
    // match partitions to accumulo tabletservers (numSplits + 1) for a given table up to maxPartitions
    val splits = client.tableOperations().listSplits(tableName, maxPartitions).asScala.toArray
    client.close()

    new java.util.ArrayList[InputPartition[InternalRow]](
      (0 to splits.length).map((i: Int) =>
        i match {
          case 0 => new PartitionReaderFactory(tableName, null, splits(i), schema, properties)
          case splits.length => new PartitionReaderFactory(tableName, splits(i - 1), null, schema, properties)
          case _ => new PartitionReaderFactory(tableName, splits(i - 1), splits(i), schema, properties)
        }
      ).asJava
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
