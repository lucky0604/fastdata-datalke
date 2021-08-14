package com.labelhub.datalake.clean_data

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.labelhub.datalake.label_data.LabelDataHandler.DLabelDetail
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object ReadHDFSToHive extends App {
  val sparkSession: SparkSession = SparkSession.builder().appName("Read HDFS To Hive").master("spark://localhost:7077").getOrCreate()
  val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  // val files = sparkSession.read.csv("hdfs://localhost:9000/user/hive/warehouse/csv/*.csv")
  val files = sparkSession.read.format("CSV")
    .option("header", "true")
    .option("inferSchema", "true")
    // .option("escape", "\"")
    .option("nullValue", "null")
    // .option("quoteAll", "true")
    .option("quoteAll", "true")
    .load("hdfs://localhost:9000/user/hive/warehouse/csv/*.csv")
  var arr: ArrayBuffer[DLabelDetail] = ArrayBuffer()
  var count : Integer = 0
  files.collect().map(rootitem => {
    val detail: JsonNode = mapper.readTree(rootitem.get(5).toString)
    println(rootitem + " " + rootitem.get(5))
    /*
    val svgArr: ArrayNode = mapper.valueToTree(detail.get("svgArr"))
    if (svgArr.size() > 0) {

      svgArr.forEach(item => {
        if (item.get("tool").asText() == "rectangle") {
          count += 1
          val xmin = List(item.get("data").get(0).get("x").asInt(), item.get("data").get(1).get("x").asInt(), item.get("data").get(2).get("x").asInt(), item.get("data").get(3).get("x").asInt()).min
          val xmax = List(item.get("data").get(0).get("x").asInt(), item.get("data").get(1).get("x").asInt(), item.get("data").get(2).get("x").asInt(), item.get("data").get(3).get("x").asInt()).max
          val ymin = List(item.get("data").get(0).get("y").asInt(), item.get("data").get(1).get("y").asInt(), item.get("data").get(2).get("y").asInt(), item.get("data").get(3).get("y").asInt()).min
          val ymax = List(item.get("data").get(0).get("y").asInt(), item.get("data").get(1).get("y").asInt(), item.get("data").get(2).get("y").asInt(), item.get("data").get(3).get("y").asInt()).max
          val width = xmax - xmin
          val height = ymax - ymin
          val color = item.get("color").asText()
          val name = item.get("name").asText()
          val isClosed = item.get("isClosed").asBoolean()
          val isShow = if (item.hasNonNull("mustShow")) item.get("mustShow").asBoolean() else true
          val uuid = item.get("uuid").asText()
          val secondaryLabelId = 0
          val labelIndex = 1
          val tool = item.get("tool").asText()
          val created_at = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(rootitem.get(7).asInstanceOf[Integer].toLong * 1000))

          arr.append(DLabelDetail(count, xmin, ymin, color, name, isClosed, isShow, labelIndex, uuid, tool, secondaryLabelId, created_at, width, height))
          println(arr)
        }
      })
    }
     */
  })
}
