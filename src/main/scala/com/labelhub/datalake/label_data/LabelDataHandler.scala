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

  case class DLabelDetail(xmin: Integer, ymin: Integer, color: String, name: String, isClosed: Boolean, isShow: Boolean, labelIndex: Integer, uuid: String, tool: String, secondaryLabelId: Integer, created_at: String, width: Integer, height: Integer, mark: Int)
  case class DUserProjectLabel(uid: Int, pid: Int, rdid: Int, label_id: String, mark: Int)
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

    /*
    val df: DataFrame = readMysqlTable(sqlContext, "product_record_detail",
      s"""
         |pid in (
         |9427,
         |9376,
         |9383,
         |9403,
         |9206,
         |9338,
         |9433,
         |9018,
         |9324,
         |9006,
         |9405,
         |9435,
         |8961,
         |9209,
         |8977,
         |9441,
         |9384,
         |8976,
         |8999,
         |9207,
         |9434,
         |8986,
         |9443,
         |9366,
         |9387,
         |9446,
         |8980,
         |8962,
         |9318,
         |9442,
         |8997,
         |8967,
         |9378,
         |9205,
         |9426,
         |9226,
         |9391,
         |9393,
         |8969,
         |9009,
         |9341,
         |9053,
         |9059,
         |9439,
         |9001,
         |9340,
         |9323,
         |8982,
         |8987,
         |9055,
         |9317,
         |9353,
         |9230,
         |9052,
         |8975,
         |8957,
         |9444,
         |9058,
         |9333,
         |8992,
         |9008,
         |9047,
         |9334,
         |9357,
         |8993,
         |9404,
         |9385,
         |9065,
         |9335,
         |9349,
         |9402,
         |9436,
         |9202,
         |9015,
         |9060,
         |9197,
         |9429,
         |9327,
         |9369,
         |8990,
         |9012,
         |9351,
         |8985,
         |9021,
         |9352,
         |9203,
         |9371,
         |9325,
         |9423,
         |9364,
         |8954,
         |8996,
         |9350,
         |8973,
         |9066,
         |9322,
         |9204,
         |9355,
         |9447,
         |9425,
         |9336,
         |9330,
         |9316,
         |9014,
         |9224,
         |9394,
         |9432,
         |8979,
         |8966,
         |9051,
         |9056,
         |9360,
         |9195,
         |9431,
         |8989,
         |9004,
         |9406,
         |9361,
         |8971,
         |8998,
         |8984,
         |9329,
         |9198,
         |9368,
         |8970,
         |9002,
         |9023,
         |9211,
         |9354,
         |9057,
         |8965,
         |9320,
         |9389,
         |9229,
         |9061,
         |8995,
         |9386,
         |8958,
         |9445,
         |9358,
         |8960,
         |9201,
         |9448,
         |8959,
         |9013,
         |9019,
         |9370,
         |9213,
         |9005,
         |9332,
         |9326,
         |9440,
         |9196,
         |9020,
         |9222,
         |9214,
         |9227,
         |9367,
         |9003,
         |9063,
         |9449,
         |9200,
         |9407,
         |9064,
         |9054,
         |9210,
         |9225,
         |9408,
         |9337,
         |9194,
         |9319,
         |8988,
         |9339,
         |9362,
         |9022,
         |9000,
         |8983,
         |9011,
         |9331,
         |9421,
         |9422,
         |8956,
         |9007,
         |8968,
         |8994,
         |8963,
         |9212,
         |9223,
         |9377,
         |9356,
         |9392,
         |9388,
         |9430,
         |9424,
         |8964,
         |8991,
         |9049,
         |9228,
         |8972,
         |9010,
         |9017,
         |9428,
         |8981,
         |9365,
         |9380,
         |9068,
         |9438,
         |9359,
         |9062,
         |8955,
         |9050,
         |9321,
         |9382,
         |9437,
         |9381,
         |9199,
         |9208,
         |9048,
         |9348,
         |9067
         |) and detail is not null and detail != ''""".stripMargin, proPath)

     */


    val df: DataFrame = readMysqlTable(sqlContext, "product_record_detail",
      s"""
         | detail is not null and detail != ''""".stripMargin, proPath)


    var labelArr: ArrayBuffer[DLabelDetail] = ArrayBuffer()
    var userProjectLabelArr: ArrayBuffer[DUserProjectLabel] = ArrayBuffer()
    df.collect().foreach(rootitem => {
      println(rootitem)
      val detail: JsonNode = mapper.readTree(rootitem.get(5).toString)
      val svgArr: ArrayNode = mapper.valueToTree(detail.get("svgArr"))
      if (svgArr.size() > 0) {
        val uid = rootitem.get(11).asInstanceOf[Int]
        val pid = rootitem.get(1).asInstanceOf[Int]
        val rdid = rootitem.get(3).asInstanceOf[Int]
        svgArr.forEach(item => {
          if (item.get("tool").asText() == "rectangle") {
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
            println(created_at)
            // mark = 0表示初始导入状态的标签 1表示修改后状态的标签
            labelArr.append(DLabelDetail(xmin, ymin, color, name, isClosed, isShow, labelIndex, uuid, tool, secondaryLabelId, created_at, width, height, 0))
            userProjectLabelArr.append(DUserProjectLabel(uid, pid, rdid, uuid, 0))
          }
        })
      }
    })

    val insertLabelData: DataFrame = sqlContext.createDataFrame(labelArr).toDF("xmin", "ymin", "color", "name", "isClosed", "isShow", "labelIndex", "uuid", "tool", "secondaryLabelId", "created_at", "width", "height", "mark")
    val insertUserProjectLabelData: DataFrame = sqlContext.createDataFrame(userProjectLabelArr).toDF("uid", "pid", "rdid", "label_id", "mark")
    insertUserProjectLabelData.createOrReplaceTempView("userProjectLabelTmp")
    insertLabelData.createOrReplaceTempView("labelTmp")
    sqlContext.sql("use dataforge;")
    sqlContext.sql("select * from labelTmp").show()
    sqlContext.sql("insert into table d_label_detail select * from labelTmp")
    sqlContext.sql("select * from userProjectLabelTmp")
    sqlContext.sql("insert into table d_user_project_label select * from userProjectLabelTmp")
    sqlContext.sql("select * from d_label_detail").count()
    sqlContext.sql("select * from d_user_project_label").count()
  }

}