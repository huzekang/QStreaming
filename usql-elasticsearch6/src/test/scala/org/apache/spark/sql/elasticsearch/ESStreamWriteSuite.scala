

package org.apache.spark.sql.elasticsearch


import java.util.concurrent.TimeUnit

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.{PipelineConfig, Settings}
import de.flapdoodle.embed.process.runtime.Network
import org.apache.spark.sql.streaming.StreamTest
import org.scalatest.BeforeAndAfter
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}


class ESStreamWriteSuite extends StreamTest with BeforeAndAfter {
  val port: Int = Network.getFreeServerPort
  var embeddedElastic: EmbeddedElastic = _

  before {
    embeddedElastic = EmbeddedElastic.builder()
      .withElasticVersion("6.8.13")
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, port )
      .withSetting(PopularProperties.CLUSTER_NAME, "test-cluster")
      .withPlugin("analysis-stempel")
      .withIndex("test")
      .withStartTimeout(10, TimeUnit.MINUTES)
      .build()
      .start()

    println("es started")

    embeddedElastic.createIndex("test")

  }

  after {
    if (embeddedElastic != null) {
      embeddedElastic.stop()
    }

  }

  test("Basic Write ElasticSearch") {
    withTempDir { checkpointDir => {
        val pipeLineConfig = PipelineConfig.fromClassPath("write/es.dsl",
          Settings.load().withValue("stream.debug", "true"),
          Map("port" -> port.toString, "checkPointDir" -> checkpointDir.getCanonicalPath))
        PipelineRunner(pipeLineConfig).run()
        val indexDocuments = embeddedElastic.fetchAllDocuments("test")
        assert(indexDocuments.size() == 10)
      }
    }

  }


}