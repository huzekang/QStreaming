package com.data.spark.test

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.PipelineConfig

import scala.io.Source

object PipelineRunnerSuite {
  def main(args: Array[String]): Unit = {
    val jobFile = Source.fromFile("/Users/huzekang/code/liantong/USQL/examples/originalSQL.dsl")
    val pipelineConfig = PipelineConfig(jobFile)
    PipelineRunner(pipelineConfig).run()
  }

}
