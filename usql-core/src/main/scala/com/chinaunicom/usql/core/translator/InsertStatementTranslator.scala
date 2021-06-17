
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.{InsertStatement, SinkTable}
import com.chinaunicom.usql.core.sink.{BatchWriter, StreamWriter}
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case class InsertStatementTranslator(insertStatement: InsertStatement) extends StatementTranslator with Logging {

  override def translate(context: PipelineContext): Unit = {
    log.debug(s"parsing insert statement:\n ${insertStatement.sql}")
    val dataFrame = context.sparkSession.sql(insertStatement.sql)
    write(insertStatement.sinkTable, dataFrame)
  }

  private def write(table: SinkTable, dataFrame: DataFrame) = {
    val writer = if (table.streaming)
      new StreamWriter
    else
      new BatchWriter
    writer.write(dataFrame, table)

  }

}
