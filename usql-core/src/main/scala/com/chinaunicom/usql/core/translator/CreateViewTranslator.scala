
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.{PipelineContext, config}
import com.chinaunicom.usql.core.config.{CreateViewStatement, RowTime, ViewType}
import com.chinaunicom.usql.core.source.WaterMarker
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CreateViewTranslator(statement: CreateViewStatement) extends StatementTranslator with WaterMarker with Logging {
  val waterMark: Option[RowTime] = statement.options.get("waterMark").map(_.split(",\\s*")) match {
    case Some(Array(fromField, eventTime, delay)) => Some(config.RowTime(fromField, eventTime, delay))
    case Some(Array(eventTime, delay)) => Some(config.RowTime(eventTime, eventTime, delay))
    case _ => None
  }


  def translate(context: PipelineContext): Unit = {
    val sparkSession = context.sparkSession
    log.debug(s"parsing query: \n${statement.sql}")
    statement.viewType match {
      case ViewType.`tempView` => {
        var table = sparkSession.sql(statement.sql)
        table = repartition(table)
        table = withWaterMark(table, waterMark)
        table.createOrReplaceTempView(statement.viewName)
      }
      case ViewType.`globalView` => {
        var table = sparkSession.sql(statement.sql)
        table = repartition(table)
        table = withWaterMark(table, waterMark)
        table.createOrReplaceGlobalTempView(statement.viewName)
      }
    }

  }


  private def repartition(table: DataFrame): DataFrame = {
    var dataFrame = statement.options.get("repartition") match {
      case Some(repartition) =>
        val partitionColumns = repartition.split(",\\s*")
        partitionColumns match {
          case Array(num) if num.forall(_.isDigit) =>
            log.debug("Repartitioned with size. repartition={}", repartition)
            table.repartition(num.toInt)
          case Array(num, tails@_*) if num.forall(_.isDigit) =>
            log.debug("Repartitioned with fix size and columns. repartition={}", repartition)
            table.repartition(num.toInt, tails.map(table.col): _*)
          case Array() =>
            log.debug("Repartition option with no effect. repartition={}", repartition)
            table
          case allColumns =>
            log.debug("Repartitioned with columns. repartition={}", repartition)
            table.repartition(allColumns.map(table.col): _*)
        }
      case None =>
        log.debug("No repartition")
        table
    }

    val coalesce: Option[Int] = statement.options.get("coalesce").map(_.toInt)
    dataFrame = coalesce.map(dataFrame.coalesce).getOrElse(dataFrame)

    dataFrame
  }
}
