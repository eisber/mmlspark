// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.accumulo

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new AccumuloDataSourceReader(schema, options)
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    throw new UnsupportedOperationException("Must supply schema")
  }

  override def createWriter(jobId: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new AccumuloDataSourceWriter(schema, mode, options))
  }
}
