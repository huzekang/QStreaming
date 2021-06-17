package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.Pipeline
import com.chinaunicom.usql.util.Logging

case class PipelineTranslator(pipeline: Pipeline) extends StatementTranslator with Logging {
  override def translate(pipelineContext: PipelineContext): Unit = pipeline.statements.foreach(_.execute(pipelineContext))
}
