package com.microsoft.ml.spark.accumulo

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.codehaus.jackson.map.ObjectMapper

import scala.beans.BeanProperty

// keeping the property names short to not hit any limits
case class SchemaMappingField(@BeanProperty val cf: String,
                              @BeanProperty val cq: String,
                              @BeanProperty val t: String)

@SerialVersionUID(1L)
object AvroUtils {
  def catalystSchemaToJson(schema: StructType): String = {

    val selectedFields = schema.fields.flatMap(cf =>
      cf.dataType match {
        case cft: StructType => cft.fields.map(cq =>
          SchemaMappingField(
            cf.name,
            cq.name,
            // TODO: toUpperCase() is weird...
            cq.dataType.typeName.toUpperCase
          )
        )
        // Skip unknown types (e.g. string for row key)
        case _ => None
        // case other => throw new IllegalArgumentException(s"Unsupported type: ${other}")
      }
    )
      .filter(p => p != None)

    try
      new ObjectMapper().writeValueAsString(selectedFields)
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  implicit class CatalystSchemaToAvroRecordBuilder(builder: SchemaBuilder.FieldAssembler[Schema]) {
    def addAvroRecordFields(schema: StructType): SchemaBuilder.FieldAssembler[Schema] = {
      schema.fields.foldLeft(builder) { (builder, field) =>
        field.dataType match {
          case DataTypes.StringType =>
            if (field.nullable)
              builder.optionalString(field.name)
            else
              builder.requiredString(field.name)

          case DataTypes.IntegerType =>
            if (field.nullable)
              builder.optionalInt(field.name)
            else
              builder.requiredInt(field.name)

          case DataTypes.FloatType =>
            if (field.nullable)
              builder.optionalFloat(field.name)
            else
              builder.requiredFloat(field.name)

          case DataTypes.DoubleType =>
            if (field.nullable)
              builder.optionalDouble(field.name)
            else
              builder.requiredDouble(field.name)

          case DataTypes.BooleanType =>
            if (field.nullable)
              builder.optionalBoolean(field.name)
            else
              builder.requiredBoolean(field.name)

          case DataTypes.LongType =>
            if (field.nullable)
              builder.optionalLong(field.name)
            else
              builder.requiredLong(field.name)

          case DataTypes.BinaryType =>
            if (field.nullable)
              builder.optionalBytes(field.name)
            else
              builder.requiredBytes(field.name)

          // TODO: date/time support?
          case _ => throw new UnsupportedOperationException(s"Unsupported type: $field.dataType")
        }
      }
    }
  }

  // TODO: can this be replaced with org.apache.avro.Schema.Parser().parse() ?
  def catalystSchemaToAvroSchema(schema: StructType): Schema = {
    val fieldBuilder = SchemaBuilder.record("root")
      .fields()

    schema.fields.foldLeft(fieldBuilder) {  (builder, field) =>
      field.dataType match {
        case cft: StructType =>
          // builder.record(field.name).fields().nullableBoolean().endRecord()
          fieldBuilder
            .name(field.name)
            .`type`(SchemaBuilder
              .record(field.name)
              .fields
              .addAvroRecordFields(cft)
              .endRecord())
            .noDefault()

        case _ => builder // just skip top-level non-struct fields
      }
    }
      .endRecord()
  }
}
