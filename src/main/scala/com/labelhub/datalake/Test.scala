package com.labelhub.datalake

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{FileInputStream, PrintWriter}
import java.util.Properties
import scala.io.Source

/**
 * @Author: lucky
 * @License: (C) Copyright
 * @Contact: lucky_soft@163.com
 * @Date: 2021/7/14 23:18
 * @Version: 1.0
 * @description:
 * */


object Test extends App{

  def testHDFS() = {
    val conf = new Configuration()
    //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("/input/file2.txt"))
    val writer = new PrintWriter(output)
    try {
      writer.write("this is a test")
      writer.write("\n")
    }
    finally {
      writer.close()
      println("Closed!")
    }
    println("Done!")
  }

  def testSpark() = {
    val conf = new SparkConf().setMaster("spark://localhost:7077")
      //.setJars(List("D:\\Ubuntu\\code\\Work\\labelhub-datalake\\out\\artifacts\\labelhub_datalake_jar\\labelhub-datalake.jar"))
      .setAppName("test spark")
      //.setIfMissing("spark.driver.host", "192.168.31.132")  // docker中的container的ip
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("hdfs://localhost:9000/input/file2.txt")
    println(textFile.count())
  }

  def testSparkHive() = {
    val spark = SparkSession.builder().appName("Test Spark Hive")
      .master("spark://localhost:7077")
      .config("hadoop.home.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use dataforge;")
    val df: DataFrame = spark.sql("select * from duser;")

    df.show()
  }

  def testReadMysql(): Unit = {
    val path = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    println(properties.get("MYSQL_HOST"))
  }
  // testHDFS()
  // testSpark()
  testSparkHive()
  // testReadMysql()

}
