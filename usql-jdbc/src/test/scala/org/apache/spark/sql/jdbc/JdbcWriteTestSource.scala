package org.apache.spark.sql.jdbc

import com.chinaunicom.usql.core.config.SourceTable
import com.chinaunicom.usql.core.source.Reader
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, SparkSession}


case class TestData(name: String, value: Long)

class JdbcWriteTestSource extends Reader {

  override def read(sparkSession: SparkSession, sourceTable: SourceTable): DataFrame = {

    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext
    val input = MemoryStream[Int]
    input.addData(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    input.toDF().map {
      row =>
        val value = row.getInt(0)
        TestData(s"name_$value", value.toLong)
    }.toDF("name", "value")
  }
}
