

package org.apache.spark.sql.hbase

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.{PipelineConfig, Settings}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, HConstants, MiniHBaseCluster, TableName}
import org.apache.spark.sql.streaming.StreamTest
import org.scalatest.BeforeAndAfter


class HBaseStreamWriteSuite extends StreamTest with BeforeAndAfter {
  var server: MiniHBaseCluster = _
  val tblName = "test"
  val cf = "cf"
  var miniServer: HBaseTestingUtility = _
  before {
    miniServer = new HBaseTestingUtility()
    server = miniServer.startMiniCluster()
    val port = miniServer.getZkCluster.getClientPort
    miniServer.createTable(TableName.valueOf(tblName), Bytes.toBytes(cf))
    println("zookeeper start on port: " + port)
    println("hbase started")
  }

  after {

  }

  test("Basic Write Hbase") {

    withTempDir { checkpointDir => {
      val pipeLineConfig = PipelineConfig.fromClassPath("write/hbase.dsl",
        Settings.load().withValue("stream.debug", "true"),
        Map("checkPointDir" -> checkpointDir.getCanonicalPath,
          "zkQuorum" -> miniServer.getConfiguration.get(HConstants.ZOOKEEPER_QUORUM),
          "zkClientPort" -> miniServer.getConfiguration.get(HConstants.ZOOKEEPER_CLIENT_PORT)
        ))
      PipelineRunner(pipeLineConfig).run()
      assert(miniServer.countRows(TableName.valueOf(tblName)) == 10)
    }
    }

  }




}