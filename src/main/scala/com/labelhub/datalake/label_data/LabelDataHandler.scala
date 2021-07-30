package com.labelhub.datalake.label_data

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.labelhub.datalake.utils.ReadMysql.readMysqlTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer



object LabelDataHandler{

  case class DLabelDetail(id: Integer, xmin: Integer, ymin: Integer, color: String, name: String, isClosed: Boolean, isShow: Boolean, labelIndex: Integer, uuid: String, tool: String, secondaryLabelId: Integer, created_at: String, width: Integer, height: Integer)
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

    val df: DataFrame = readMysqlTable(sqlContext, "product_record_detail",
      s"""
         |pid in (8922,
         |8924,
         |8925,
         |8926,
         |8934,
         |8935,
         |8923,
         |8927,
         |8928,
         |8929,
         |8930,
         |8931,
         |8955,
         |8958,
         |8961,
         |8963,
         |8965,
         |8967,
         |8970,
         |8974,
         |8975,
         |8976,
         |8977,
         |8954,
         |8956,
         |8957,
         |8959,
         |8960,
         |8962,
         |8964,
         |8966,
         |8968,
         |8969,
         |8971,
         |8973,
         |8981,
         |8982,
         |8983,
         |8984,
         |8985,
         |8986,
         |8987,
         |8988,
         |8989,
         |8990,
         |8991,
         |8992,
         |8993,
         |8994,
         |8995,
         |8996,
         |8997,
         |8998,
         |8999,
         |9000,
         |9001,
         |9002,
         |9003,
         |9004,
         |9005,
         |9006,
         |9007,
         |9008,
         |9009,
         |9010,
         |9011,
         |9012,
         |9013,
         |9014,
         |9015,
         |9017,
         |9018,
         |9019,
         |9020,
         |9021,
         |9022,
         |9023,
         |9047,
         |9048,
         |9049,
         |9050,
         |9051,
         |9052,
         |9053,
         |9054,
         |9055,
         |9056,
         |9057,
         |9058,
         |9059,
         |9060,
         |9061,
         |9062,
         |9063,
         |9064,
         |9065,
         |9066,
         |9067,
         |9068,
         |9194,
         |9195,
         |9196,
         |9197,
         |9198,
         |9199,
         |9200,
         |9201,
         |9202,
         |9203,
         |9204,
         |9205,
         |9206,
         |9207,
         |9208,
         |9209,
         |9210,
         |9211,
         |9212,
         |9213,
         |9214,
         |9222,
         |9223,
         |9224,
         |9225,
         |9226,
         |9227,
         |9228,
         |9229,
         |9230,
         |9323,
         |9324,
         |9325,
         |9326,
         |9327,
         |9329,
         |9330,
         |9331,
         |9332,
         |9333,
         |9334,
         |9335,
         |9336,
         |9337,
         |9338,
         |9339,
         |9340,
         |9341,
         |9316,
         |9317,
         |9318,
         |9319,
         |9320,
         |9321,
         |9322,
         |9348,
         |9349,
         |9350,
         |9351,
         |9352,
         |9353,
         |9354,
         |9355,
         |9356,
         |9357,
         |9358,
         |9359,
         |9360,
         |9361,
         |9362,
         |9364,
         |9365,
         |9366,
         |9367,
         |9368,
         |9369,
         |9370,
         |9371,
         |9376,
         |9377,
         |9378,
         |9380,
         |9381,
         |9382,
         |9383,
         |9384,
         |9385,
         |9386,
         |9387,
         |9388,
         |9389,
         |9391,
         |9392,
         |9393,
         |9394,
         |9439,
         |9440,
         |9441,
         |9442,
         |9443,
         |9444,
         |9445,
         |9446,
         |9447,
         |9448,
         |9449,
         |9402,
         |9403,
         |9404,
         |9405,
         |9406,
         |9407,
         |9408,
         |9421,
         |9422,
         |9423,
         |9424,
         |9425,
         |9426,
         |9427,
         |9428,
         |9429,
         |9430,
         |9431,
         |9432,
         |9433,
         |9434,
         |9435,
         |9436,
         |9437,
         |9438,
         |10509,
         |10510,
         |10511,
         |10512,
         |10513,
         |10514,
         |10518,
         |10519,
         |10520,
         |10521,
         |10522,
         |10523,
         |10524,
         |10525,
         |10526,
         |10527,
         |10528,
         |10529) and detail is not null and detail != ''""".stripMargin, proPath)
    var arr: ArrayBuffer[DLabelDetail] = ArrayBuffer()
    var count : Integer = 0
    df.collect().foreach(rootitem => {
      println(rootitem)
      val detail: JsonNode = mapper.readTree(rootitem.get(5).toString)
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
            val created_at = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rootitem.get(7).asInstanceOf[Integer])
            arr.append(DLabelDetail(count, xmin, ymin, color, name, isClosed, isShow, labelIndex, uuid, tool, secondaryLabelId, created_at, width, height))
          }
        })
      }
    })
    val insertData: DataFrame = sqlContext.createDataFrame(arr).toDF("id", "xmin", "ymin", "color", "name", "isClosed", "isShow", "labelIndex", "uuid", "tool", "secondaryLabelId", "created_at", "width", "height")
    insertData.createOrReplaceTempView("tmpv")
    sqlContext.sql("use dataforge;")
    sqlContext.sql("select * from tmpv").show()
    sqlContext.sql("insert into table d_label_detail select * from tmpv")
    sqlContext.sql("select * from d_label_detail").show()
  }
}