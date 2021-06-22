package com.chinaunicom.usql.faker

import org.apache.spark.sql.{DataFrame, SparkSession}

object FakerSourceTest extends App{
  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

   val df: DataFrame = spark.read.format("faker")
     .schema("id int ,name string,isMan boolean,salary double")
     .option("data",
       """
         |1,pk,false,4322.3
         |2,didi,true,312.2
         |3,kkk,false,222.0
         |""".stripMargin)
     .load()

  df.printSchema()
  df.show()

  df.select("id").show()

  spark.stop()
}
