/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qiniu.stream.core

import com.qiniu.stream.core.config.{DebugEnable, Pipeline, PipelineConfig, Settings}
import com.qiniu.stream.core.parser.PipelineParser
import com.qiniu.stream.util.Logging

case class PipelineRunner(pipelineConfig: PipelineConfig) extends Logging {

  var settings: Settings = pipelineConfig.settings

  lazy val pipelineContext: PipelineContext = PipelineContext(settings)

  def run(): Unit = {
    val version = "v0.2"
    log.info(
      s"""
        | _   _ ____   ___  _
        | | | | / ___| / _ \| |
        | | | | \___ \| | | | |
        | | |_| |___) | |_| | |___
        |  \___/|____/ \__\_\_____|         version $version
        |
        |""".stripMargin)
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


