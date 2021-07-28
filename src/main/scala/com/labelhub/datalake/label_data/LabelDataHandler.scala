package com.labelhub.datalake.label_data

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.labelhub.datalake.utils.ReadMysql.readMysqlTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer



object LabelDataHandler{

  def main(args: Array[String]): Unit = {
    val sqlContext: SparkSession = SparkSession.builder().appName("Test Spark Hive")
      .master("spark://localhost:7077")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.driver.memory", "6g")
      .config("spark.executor.memory", "6g")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .enableHiveSupport()
      .getOrCreate()
    // json mapper
    val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val proPath = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath

    val df: DataFrame = readMysqlTable(sqlContext, "product_record_detail", s"detail is not null and detail != '' limit 10", proPath)

    df.collect().foreach(item => {
      val detail: JsonNode = mapper.readTree(item.get(5).toString)
      val svgArr: ArrayNode = mapper.valueToTree(detail.get("svgArr"))
      if (svgArr.size() > 0) {
        var data = Array((1, 10, 10, "blue", "test", true, true, 1, "testuuid", "test tool", 1, "2021-02-02 18:29:09", 100, 100))

        svgArr.forEach(svgItem => {

        })


        val df: DataFrame = sqlContext.createDataFrame(data).toDF("id", "xmin", "ymin", "color", "name", "isClosed", "isShow", "labelIndex", "uuid", "tool", "secondaryLabelId", "created_at", "width", "height")

        df.createOrReplaceTempView("tmpv")
        sqlContext.sql("use dataforge;")
        //sqlContext.sql("select * from tmpv").show()
        //sqlContext.sql("insert into table d_label_detail select * from tmpv")
        sqlContext.sql("select * from d_label_detail").show()
      }
    })
  }
}