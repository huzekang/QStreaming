package com.chinaunicom.usql.faker

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.PipelineConfig

import scala.io.Source

object USQLFakerTest extends App {
  val pipelineConfig = PipelineConfig(Source.fromFile("/Users/huzekang/code/liantong/USQL/examples/faker2console.dsl"))
  PipelineRunner(pipelineConfig).run()

}
