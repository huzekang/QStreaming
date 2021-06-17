
package com.chinaunicom.usql.core.source

import com.chinaunicom.usql.core.config.SourceTable
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  def read(sparkSession: SparkSession,sourceTable:SourceTable): DataFrame
}

