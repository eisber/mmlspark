// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import com.microsoft.ml.spark.core.env.StreamUtilities
import org.apache.accumulo.core.client.Accumulo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import scala.collection.JavaConverters._

// TODO: https://github.com/apache/spark/blob/053dd858d38e6107bc71e0aa3a4954291b74f8c8/sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/SupportsReportPartitioning.java
// in head of spark github repo
// import org.apache.spark.sql.connector.read.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}

import org.apache.hadoop.io.Text

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(1L)
class AccumuloDataSourceReader(schema: StructType, options: DataSourceOptions)
  extends DataSourceReader with Serializable with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  private val defaultMaxPartitions = 200

  var filters = Array.empty[Filter]

  val rowKeyColumn = options.get("rowKey").orElse("rowKey")

  // needs to be nullable so that Avro doesn't barf when we want to add another column
  private var requiredSchema = schema.add(rowKeyColumn, DataTypes.StringType, true)

  private val schemaWithoutRowKey = new StructType(schema.fields.filter(_.name != rowKeyColumn))
  private val jsonSchema = AvroUtils.catalystSchemaToJson(schemaWithoutRowKey)

  private var filterInJuel: Option[String] = None

  // SupportsPushDownRequiredColumns implementation
  override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
  }

  def readSchema: StructType = requiredSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // unfortunately predicates on nested elements are not pushed down by Spark
    // https://issues.apache.org/jira/browse/SPARK-17636
    // https://github.com/apache/spark/pull/22535
    // val filtersStr = filters.mkString(",")
    // println(s"\nINPUT FILTER: ${filtersStr}")

    val result = new FilterToJuel(jsonSchema.attributeToVariableMapping).serializeFilters(filters)

    this.filters = result.supportedFilters.toArray

    if (this.filters.length > 0)
      this.filterInJuel = Some(result.serializedFilter)

    result.unsupportedFilters.toArray
  }

  override def pushedFilters(): Array[Filter] = filters

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    val maxPartitions = options.getInt("maxPartitions", defaultMaxPartitions) - 1
    val properties = new java.util.Properties()
    // can use .putAll(options.asMap()) due to https://github.com/scala/bug/issues/10418
    options.asMap.asScala.foreach { case (k, v) => properties.setProperty(k, v) }

    val splits = ArrayBuffer(new Text("-inf").getBytes, new Text("inf").getBytes)
    splits.insertAll(1,
      StreamUtilities.using(Accumulo.newClient().from(properties).build()) { client =>
        client.tableOperations().listSplits(tableName, maxPartitions)
      }
        .get
        .asScala
        .map(_.getBytes)
    )

    new java.util.ArrayList[InputPartition[InternalRow]](
      (1 until splits.length).map(i =>
        new PartitionReaderFactory(tableName, splits(i - 1), splits(i), requiredSchema, properties, rowKeyColumn,
          jsonSchema.json, filterInJuel)
      ).asJava
    )
  }
}

class PartitionReaderFactory(tableName: String,
                             start: Array[Byte],
                             stop: Array[Byte],
                             schema: StructType,
                             properties: java.util.Properties,
                             rowKeyColumn: String,
                             jsonSchema: String,
                             filterInJuel: Option[String])
  extends InputPartition[InternalRow] {
  def createPartitionReader: InputPartitionReader[InternalRow] = {
    new AccumuloInputPartitionReader(tableName, start, stop, schema, properties, rowKeyColumn, jsonSchema, filterInJuel)
  }
}
