package com.chinaunicom.usql.core

import com.chinaunicom.usql.core.config.{HiveEnable, Settings}
import com.chinaunicom.usql.core.config.Settings
import com.chinaunicom.usql.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

case class PipelineContext(settings: Settings) extends Logging {
  val master = if (settings.config.hasPath("spark.master")) {
    settings.config.getString("spark.master")
  } else {
    "local"
  }

  lazy val sparkSession: SparkSession = {
    val sparkConf: SparkConf = {
      val sparkConf = new SparkConf()
      if (settings.config.hasPath("spark")) {
        import scala.collection.JavaConversions._
        settings.config.getConfig("spark").entrySet().foreach(e => {
          sparkConf.set(e.getKey, settings.config.getString("spark." + e.getKey))
        })
      }
      sparkConf
    }

    val builder = SparkSession.builder().config(sparkConf).master(master)
    if (settings.config.hasPath(HiveEnable.name) && settings(HiveEnable)) builder.enableHiveSupport()
    builder.getOrCreate()
  }

  def stop = {
    Try {
      sparkSession.stop
    } match {
      case Success(_) =>
      case Failure(e) => logError("unexpected error while shutdown", e)
    }
  }

}