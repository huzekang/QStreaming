

package com.chinaunicom.usql.jdbc

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
class JdbcSourceProvider extends StreamWriteSupport with DataSourceRegister{
  override def createStreamWriter(queryId: String, schema: StructType,
    mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    val optionMap = options.asMap().asScala.toMap
    // add this for parameter check.
    new JDBCOptions(optionMap)
    new JdbcStreamWriter(schema, optionMap)
  }

  // short name 'jdbc' is used for batch, chose a different name for streaming.
  override def shortName(): String = "streaming-jdbc"
}