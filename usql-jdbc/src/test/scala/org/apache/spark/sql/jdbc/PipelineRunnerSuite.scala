package org.apache.spark.sql.jdbc

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.PipelineConfig
import org.apache.spark.sql.streaming.StreamTest

import scala.io.Source

class PipelineRunnerSuite extends StreamTest {
  test("pipelineRunner test") {
    withTempDir { checkpointDir => {
      val pipelineConfig = PipelineConfig(Source.fromFile("/Users/huzekang/code/liantong/USQL/examples/socket2console.dsl"))
      PipelineRunner(pipelineConfig).run()

    }
    }

  }
}
