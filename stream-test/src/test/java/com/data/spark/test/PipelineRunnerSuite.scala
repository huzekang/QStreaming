package com.data.spark.test

import com.qiniu.stream.core.PipelineRunner
import com.qiniu.stream.core.config.PipelineConfig

import scala.io.Source

object PipelineRunnerSuite {
  def main(args: Array[String]): Unit = {
    val jobFile = Source.fromFile("/Volumes/Samsung_T5/opensource/QStreaming/examples/textstream2console.dsl")
    val pipelineConfig = PipelineConfig(jobFile)
    PipelineRunner(pipelineConfig).run()
  }

}
