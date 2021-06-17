
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.util.Logging

trait StatementTranslator extends Logging {
  def translate(pipelineContext: PipelineContext)
}

