package com.labelhub.datalake.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.io.FileInputStream
import java.util.Properties


object ReadMysql extends App {
  val sqlContext: SparkSession = SparkSession.builder().appName("Test Spark Hive")
    .master("spark://localhost:7077")
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .enableHiveSupport()
    .getOrCreate()

  def readMysql(sqlContext: SparkSession, tableName: String, proPath: String): DataFrame = {
    val properties = getProperties(proPath)
    sqlContext.read
      .format("jdbc")
      .option("url", properties.getProperty("MYSQL_HOST"))
      .option("user", properties.getProperty("MYSQL_USERNAME"))
      .option("password", properties.getProperty("MYSQL_PASSWORD"))
      .option("dbtable", tableName)
      .load()
  }

  def readMysqlTable(sqlContext: SparkSession, table: String, filterCondition: String, proPath: String): DataFrame = {
    val properties = getProperties(proPath)
    var tableName = ""
    tableName = "(select * from " + table + " where " + filterCondition + " ) as t1"
    sqlContext.read
      .format("jdbc")
      .option("url", properties.getProperty("MYSQL_HOST"))
      .option("user", properties.getProperty("MYSQL_USERNAME"))
      .option("password", properties.getProperty("MYSQL_PASSWORD"))
      .option("dbtable", tableName)
      .load()
  }

  def getProperties(proPath: String): Properties = {
    val properties: Properties = new Properties()
    properties.load(new FileInputStream(proPath))
    properties
  }

  val proPath = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
  /**
  val df: DataFrame = readMysql(sqlContext, "user", proPath)
  df.select(
    "name"
  ).limit(10).show(false)
  */
  val df: DataFrame = readMysqlTable(sqlContext, "product_record_detail", s"detail is not null and detail != ''", proPath)
  df.select("detail").show(10)
}
