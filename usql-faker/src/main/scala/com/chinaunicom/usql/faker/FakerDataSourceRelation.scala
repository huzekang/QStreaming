package com.chinaunicom.usql.faker

import com.google.gson.{Gson, JsonObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 1 继承BaseRelation 声明scheme
 * 2 继承Serializable
 * 3 TableScan 读取数据
 * 4 PrunedScan 指定列读取数据 (减少从数据源获取到spark的数据量)
 */
class FakerDataSourceRelation(sqlContextObject: SQLContext, data: String, userSchema: StructType)
  extends BaseRelation with Serializable with TableScan with PrunedScan {
  override def sqlContext: SQLContext = this.sqlContextObject

  override def schema: StructType = userSchema

  override def buildScan(): RDD[Row] = {
    val line = data.split("\n")
    val rows = line.filter(StringUtils.isNoneBlank(_)).map(l => {
      // 取得一行每个字段的数据
      val splits = l.split(",").zipWithIndex.map {
        case (colData, index) => DataTypeUtil.castTo(colData, userSchema(index).dataType)
      }
      Row.fromSeq(Seq(splits: _*))
    })
    sqlContextObject.sparkContext.parallelize(rows)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val line = data.split("\n")
    val rows = line.filter(StringUtils.isNoneBlank(_)).map(l => {
      // 取得一行每个字段的数据
      val splits: Array[Option[Any]] = l.split(",").zipWithIndex.map {
        case (colData, index) =>
          val columnName = userSchema(index).name
          val castValue = DataTypeUtil.castTo(colData, userSchema(index).dataType)
          if (requiredColumns.contains(columnName)) {
            Some(castValue)
          } else {
            None
          }
      }
      Row.fromSeq(splits.filter(_.isDefined).map(_.get))
    })
    sqlContextObject.sparkContext.parallelize(rows)
  }
}
