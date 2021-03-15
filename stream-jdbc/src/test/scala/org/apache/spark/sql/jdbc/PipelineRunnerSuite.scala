package org.apache.spark.sql.jdbc

import com.qiniu.stream.core.PipelineRunner
import com.qiniu.stream.core.config.PipelineConfig
import org.apache.spark.sql.streaming.StreamTest

import scala.io.Source

class PipelineRunnerSuite extends StreamTest {
  test("pipelineRunner test") {
    withTempDir { checkpointDir => {
      val pipelineConfig = PipelineConfig(Source.fromFile("/Volumes/Samsung_T5/opensource/QStreaming/examples/socket2console.dsl"))
      PipelineRunner(pipelineConfig).run()

    }
    }

  }
}
