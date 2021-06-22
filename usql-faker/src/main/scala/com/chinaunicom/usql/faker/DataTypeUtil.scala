package com.chinaunicom.usql.faker

import org.apache.spark.sql.types._

object DataTypeUtil {

  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case IntegerType => value.toInt
      case StringType => value
      case LongType => value.toLong
      case BooleanType => value.toBoolean
      case DoubleType => value.toDouble
    }
  }

}
