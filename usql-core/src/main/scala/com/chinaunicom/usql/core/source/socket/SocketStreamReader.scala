package com.chinaunicom.usql.core.source.socket

import com.chinaunicom.usql.core.config.SourceTable
import com.chinaunicom.usql.core.source.{Reader, WaterMarker}
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

class SocketStreamReader extends Reader with WaterMarker with Logging {


  override def read(sparkSession: SparkSession,sourceTable: SourceTable): DataFrame = {
    sparkSession
      .readStream
      .format(sourceTable.connector.name)
      .options(sourceTable.connector.options)
      .load()
  }


}
