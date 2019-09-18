package com.microsoft.ml.spark.accumulo

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.codehaus.jackson.map.{ObjectMapper}
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion

import scala.beans.BeanProperty

// keeping the property names short to not hit any limits
case class SchemaMappingField(@BeanProperty val cf: String, // column family
                              @BeanProperty val cq: String, // column qualifier
                              @BeanProperty val fvn: String, // filter variable name
                              @BeanProperty val t: String) // type

@SerialVersionUID(1L)
object AvroUtils {
  def catalystSchemaToJson(schema: StructType): String = {

    var i = 0
    val selectedFields = schema.fields.flatMap(cf =>
      cf.dataType match {
        case cft: StructType => cft.fields.map(cq =>
          SchemaMappingField(
            cf.name,
            cq.name,
            { i += 1; s"v${i}" },
            // TODO: toUpperCase() is weird...
            cq.dataType.typeName.toUpperCase
          )
        )
        case _: DataType => Seq(SchemaMappingField(
            cf.name,
            null,
            { i += 1; s"v${i}" },
            // TODO: toUpperCase() is weird...
            cf.dataType.typeName.toUpperCase
          ))
      })

    try {
      val mapper = new ObjectMapper()

      // disable serialization of null-values
      mapper.setSerializationInclusion(Inclusion.NON_NULL)

      mapper.writeValueAsString(selectedFields)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(e)
    }
  }

  implicit class CatalystSchemaToAvroRecordBuilder(builder: SchemaBuilder.FieldAssembler[Schema]) {
    def addAvroRecordFields(schema: StructType): SchemaBuilder.FieldAssembler[Schema] = {
      schema.fields.foldLeft(builder) { (builder, field) =>
          (field.dataType, field.nullable) match {
            case (DataTypes.BinaryType, true) => builder.optionalBytes(field.name)
            case (DataTypes.BinaryType, false) => builder.requiredBytes(field.name)
            case (DataTypes.BooleanType, true) => builder.optionalBoolean(field.name)
            case (DataTypes.BooleanType, false) => builder.requiredBoolean(field.name)
            case (DataTypes.DoubleType, true) => builder.optionalDouble(field.name)
            case (DataTypes.DoubleType, false) => builder.requiredDouble(field.name)
            case (DataTypes.FloatType, true) => builder.optionalFloat(field.name)
            case (DataTypes.FloatType, false) => builder.requiredFloat(field.name)
            case (DataTypes.IntegerType, true) => builder.optionalInt(field.name)
            case (DataTypes.IntegerType, false) => builder.requiredInt(field.name)
            case (DataTypes.LongType, true) => builder.optionalLong(field.name)
            case (DataTypes.LongType, false) => builder.requiredLong(field.name)
            case (DataTypes.StringType, true) => builder.optionalString(field.name)
            case (DataTypes.StringType, false) => builder.requiredString(field.name)
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

    schema.fields.foldLeft(fieldBuilder) { (builder, field) =>
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
