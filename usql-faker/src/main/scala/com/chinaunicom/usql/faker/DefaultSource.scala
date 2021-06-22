package com.chinaunicom.usql.faker

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * 1 继承SchemaRelationProvider可以获得用户填的schema信息
 * 2 继承DataSourceRegister 声明format的名字
 */
class DefaultSource extends  SchemaRelationProvider with DataSourceRegister{

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val data = parameters.get("data")
    data match {
      case Some(d) => new FakerDataSourceRelation(sqlContext,d,schema)
      case None =>  throw new IllegalArgumentException("data is required from faker-datasource format")
    }
  }

  override def shortName(): String = "faker"
}
