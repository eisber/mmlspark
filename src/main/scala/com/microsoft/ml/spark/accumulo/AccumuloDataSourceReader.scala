// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import com.microsoft.ml.spark.core.env.StreamUtilities
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

    var splits = StreamUtilities.using(Accumulo.newClient().from(properties).build()) { client =>
      client.tableOperations().listSplits(tableName, maxPartitions)
    }.get.asScala.map(_.getBytes).toArray

    if (splits.length == 0)
      splits = Array(null)

    new java.util.ArrayList[InputPartition[InternalRow]](
      (0 until splits.length).map((i: Int) =>
        i match {
          case 0 => new PartitionReaderFactory(tableName, null, splits(i), schema, properties)
          case _ => new PartitionReaderFactory(tableName, splits(i - 1), splits(i), schema, properties)
        }
      ).asJava
    )
  }
}

class PartitionReaderFactory(tableName: String,
                             start: Array[Byte],
                             stop: Array[Byte],
                             schema: StructType,
                             properties: java.util.Properties)
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, start, stop, schema, properties)
  }
}
