package com.labelhub.datalake.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.io.FileInputStream
import java.util.Properties


object ReadMysql extends App {


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
      .option("numPartitions", 20)
      .option("partitionColumn", "crtime")
      .option("lowerBound", "0")
      .option("upperBound", "2000")
      .option("dbtable", tableName)
      .load()
  }

  def getProperties(proPath: String): Properties = {
    val properties: Properties = new Properties()
    properties.load(new FileInputStream(proPath))
    properties
  }


}
