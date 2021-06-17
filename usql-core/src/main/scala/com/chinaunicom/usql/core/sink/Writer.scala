
package com.chinaunicom.usql.core.sink

import com.chinaunicom.usql.core.config.SinkTable
import org.apache.spark.sql.DataFrame


trait Writer {
  def write(dataFrame: DataFrame,sinkTable: SinkTable)
}
