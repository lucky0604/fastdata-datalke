package com.labelhub.datalake.clean_data

import org.apache.spark.sql.SparkSession

object CountLabel extends App {

  val spark: SparkSession = SparkSession.builder().appName("Test Spark Hive")
    .master("spark://localhost:7077")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.sql.storeAssignmentPolicy", "LEGACY")
    .enableHiveSupport()
    .getOrCreate()
  spark.sql("use dataforge;")
  val sum = spark.sql("select distinct uuid from d_label_detail").count()
  println(sum)
}
