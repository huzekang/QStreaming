
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.SinkTable

case class SinkTableTranslator(sinkTable:SinkTable) extends StatementTranslator {

  override def translate(pipelineContext: PipelineContext): Unit = {
    //no op
  }
}
