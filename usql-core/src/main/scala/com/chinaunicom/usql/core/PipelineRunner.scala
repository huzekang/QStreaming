
package com.chinaunicom.usql.core

import com.chinaunicom.usql.core.config.{DebugEnable, Pipeline, PipelineConfig, Settings}
import com.chinaunicom.usql.core.config.{Pipeline, PipelineConfig, Settings}
import com.chinaunicom.usql.core.parser.PipelineParser
import com.chinaunicom.usql.util.Logging

case class PipelineRunner(pipelineConfig: PipelineConfig) extends Logging {

  var settings: Settings = pipelineConfig.settings

  lazy val pipelineContext: PipelineContext = PipelineContext(settings)

  def run(): Unit = {
    log.info(
      """
         _   _ ____   ___  _
         | | | / ___| / _ \| |
         | | | \___ \| | | | |
         | |_| |___) | |_| | |___
          \___/|____/ \__\_\_____|             version V1.0.0

      """)
    val pipeline = new PipelineParser(pipelineConfig).parse()
    run(pipeline)
  }

  def run(pipeline: Pipeline): Unit = {
    def awaitTermination() {
      val sparkSession = pipelineContext.sparkSession
      if (sparkSession.streams.active.nonEmpty) {
        val debug = settings.config.hasPath(DebugEnable.name) && settings(DebugEnable)
        if (debug) {
          sparkSession.streams.active.foreach(_.processAllAvailable())
        } else {
          sparkSession.streams.awaitAnyTermination()
        }
      }
    }

    pipeline.execute(pipelineContext)
    awaitTermination()
  }

}


