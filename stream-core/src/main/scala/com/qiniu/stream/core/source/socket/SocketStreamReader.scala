package com.qiniu.stream.core.source.socket

import com.qiniu.stream.core.config.SourceTable
import com.qiniu.stream.core.source.{Reader, WaterMarker}
import com.qiniu.stream.util.Logging
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
