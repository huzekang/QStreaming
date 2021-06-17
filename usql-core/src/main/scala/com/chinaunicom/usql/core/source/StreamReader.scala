
package com.chinaunicom.usql.core.source

import com.chinaunicom.usql.core.config.SourceTable
import com.chinaunicom.usql.core.source.file.FileStreamReader
import com.chinaunicom.usql.core.source.kafka.KafkaStreamReader
import com.chinaunicom.usql.core.source.redis.RedisStreamReader
import com.chinaunicom.usql.core.source.socket.SocketStreamReader
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

class StreamReader extends Reader with WaterMarker with Logging {

  lazy val streamReaders = Map(
    "kafka"->new KafkaStreamReader,
    "file"-> new FileStreamReader,
    "csv"-> new FileStreamReader,
    "json"-> new FileStreamReader,
    "text"-> new FileStreamReader,
    "socket"-> new SocketStreamReader,
    "redis"-> new RedisStreamReader
  )

  override def read(sparkSession: SparkSession, sourceTable: SourceTable): DataFrame = {

    streamReaders.get(sourceTable.connector.name) match {
      case Some(reader)=> reader.read(sparkSession,sourceTable)
      case None=> throw new UnsupportedOperationException(s"${sourceTable.connector.name} is not supported")
    }
  }
}