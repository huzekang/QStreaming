
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.SourceTable
import com.chinaunicom.usql.core.source.{BatchReader, Reader, StreamReader}

case class SourceTableTranslator(table:SourceTable) extends StatementTranslator {
  override def translate(context: PipelineContext): Unit = {
    val reader = if (table.connector.isCustom) {
      require(table.connector.reader.isDefined,"reader is required when using custom format")
      Class.forName(table.connector.reader.get).newInstance().asInstanceOf[Reader]
    } else {
      if (!table.streaming)
        new BatchReader
      else {
        new StreamReader
      }
    }
    val dataFrame = reader.read(context.sparkSession, table)
    dataFrame.createOrReplaceTempView(table.name)
  }
}
