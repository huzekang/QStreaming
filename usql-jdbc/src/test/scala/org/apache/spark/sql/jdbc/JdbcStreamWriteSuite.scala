

package org.apache.spark.sql.jdbc

import java.sql.DriverManager

import com.chinaunicom.usql.core.PipelineRunner
import com.chinaunicom.usql.core.config.{PipelineConfig, Settings}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter


class JdbcStreamWriteSuite extends StreamTest with BeforeAndAfter {

  val url = "jdbc:h2:mem:testdb"
  val jdbcTableName = "stream_test_table"
  val driverClassName = "org.h2.Driver"
  val createTableSql =
    s"""
       |CREATE TABLE ${jdbcTableName}(
       | name VARCHAR(32),
       | value LONG,
       | PRIMARY KEY (name)
       |)""".stripMargin

  var conn: java.sql.Connection = null

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

    override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
  }

  before {
    Utils.classForName(driverClassName)
    conn = DriverManager.getConnection(url)
    conn.prepareStatement(createTableSql).executeUpdate()
  }

  after {
    conn.close()
  }

  test("Basic Write Jdbc") {
    withTempDir { checkpointDir => {
      val pipeLineConfig = PipelineConfig.fromClassPath("write/jdbc.dsl",
        Settings.load().withValue("stream.debug", "true"),
        Map(  "checkPointDir" -> checkpointDir.getCanonicalPath))
      PipelineRunner(pipeLineConfig).run()
      val result = conn
        .prepareStatement(s"select count(*) as count from $jdbcTableName")
        .executeQuery()
      assert(result.next())
      assert(result.getInt("count") == 10)
    }}

  }


}