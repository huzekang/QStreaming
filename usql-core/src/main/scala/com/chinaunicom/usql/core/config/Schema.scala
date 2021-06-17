
package com.chinaunicom.usql.core.config

import org.apache.spark.sql.types.StructType

sealed trait TimeField

case class ProcTime(fieldName: String) extends TimeField

case class RowTime(fromField: String, eventTime: String, delayThreadsHold: String) extends TimeField

case class SchemaField(fieldName: String, fieldType: String)

case class Schema(fields: Seq[SchemaField], timeField: Option[TimeField] = None)


object Schema {

  implicit class RichSchema(schema: Schema) {
    def structType: StructType = {
      StructType.fromDDL(toDDL)
    }

    def toDDL: String = {
      schema.fields.map(field => s"${field.fieldName} ${field.fieldType}").mkString(",")
    }
  }

}
