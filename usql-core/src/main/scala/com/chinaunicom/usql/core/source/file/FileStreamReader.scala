package com.chinaunicom.usql.core.source.file

import com.chinaunicom.usql.core.config.SourceTable
import com.chinaunicom.usql.core.source.{Reader, WaterMarker}
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileStreamReader extends Reader with WaterMarker with Logging {


  override def read(sparkSession: SparkSession,sourceTable: SourceTable): DataFrame = {
    require(sourceTable.schema.isDefined, "schema  is required")
    sparkSession
      .readStream.
      format(sourceTable.connector.name)
      .options(sourceTable.connector.options)
      .schema(sourceTable.schema.get.structType)
      .load()
  }


}
