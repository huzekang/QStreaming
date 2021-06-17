

package org.apache.spark.sql.hbase

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.{PipelineConfig, Settings}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{HBaseTestingUtility, MiniHBaseCluster, TableName}
import org.scalatest.{BeforeAndAfter, FunSuite}


class Test extends FunSuite with BeforeAndAfter {
  var server: MiniHBaseCluster = _
  var zkServer: MiniZooKeeperCluster = _

  val tblName = "test"
  val cf = "cf"
  var miniServer: HBaseTestingUtility = _

  before {
    miniServer = new HBaseTestingUtility()
    server = miniServer.startMiniCluster()
    println("hbase started")
  }

  after {

  }

  test("Basic Write Hbase") {
    val table = miniServer.createTable(TableName.valueOf(tblName), Bytes.toBytes(cf))
    val pipeLineConfig = PipelineConfig.fromClassPath("write/hbase.dsl",
      Settings.load().withValue("stream.debug", "true"),
      Map("checkPointDir" -> "test"))
    PipelineRunner(pipeLineConfig).run()
    assert(miniServer.countRows(table) == 10)

  }


}