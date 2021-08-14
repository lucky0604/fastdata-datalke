package com.labelhub.datalake.label_data

import com.labelhub.datalake.utils.ReadMysql.readMysqlTable
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object DataSearch extends App {

  val spark = SparkSession.builder()
    .master("spark://localhost:7077")
    .enableHiveSupport()
    .appName("Create User Table")
    .getOrCreate()

  spark.sql("use dataforge;")

  def searchOldLabel() = {
    val result = spark.sql(
      """
        |
        | select n.* from d_user_project_label n
        | left join d_user_project_label m on n.label_id = m.label_id where m.mark = 1 and n.mark = 1 and n.rdid in (select rdid from d_user_project_label where mark = 0)
        |""".stripMargin)
    println(result.count())
  }

  // project new label sum
  def projectNewLabel() = {
    val labelList = spark.sql(
      """
        | select pid, count(label_id) from d_user_project_label where mark = 1 and pid in (select pid from d_user_project_label where mark = 0)
        |  and label_id not in (select label_id from d_user_project_label where mark = 0)
        |  group by pid
        |""".stripMargin).collect()


    val filename = "./result.txt"
    val file = new File(filename)
    val writer_name = new BufferedWriter(new FileWriter(file))
    writer_name.write("项目id" + " " + "新增标签数" + "\n")
    labelList.foreach(item => {
      writer_name.write(item.get(0).toString + " " + item.get(1).toString + "\n")
    })

    writer_name.close()

  }

  // user delete label sum
  def userNewLabel() = {
    val userList = spark.sql(
      """
        | select distinct pid from d_user_project_label
        |""".stripMargin).collect()

    val oldList = userList.filter(i => i.get(4).asInstanceOf[Int] == 0)
    val newList = userList.filter(i => i.get(4).asInstanceOf[Int] == 1)
    oldList.map(i => )
  }

  // projectNewLabel()
  userNewLabel()

}
