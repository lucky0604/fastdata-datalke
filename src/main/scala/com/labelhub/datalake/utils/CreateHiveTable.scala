package com.labelhub.datalake.utils

import org.apache.spark.sql.SparkSession



object CreateHiveTable extends App {

  def createUserTable(): Unit = {
    val spark = SparkSession.builder()
      .master("spark://localhost:7077")
      .enableHiveSupport()
      .appName("Create User Table")
      .getOrCreate()

    spark.sql("use dataforge;")

    // create label table
    spark.sql(
      """
        CREATE TABLE IF NOT EXISTS d_label_detail (
        | xmin int, ymin int, color string,
        | name string, isClosed boolean, isShow boolean,
        | labelIndex int, uuid string,
        | tool string, secondaryLabelId int, created_at date,
        | width int, height int, mark int
        | ) STORED AS PARQUET
        |""".stripMargin
    )

    // create user - project - image - label relationship table
    spark.sql(
      """
        | CREATE TABLE IF NOT EXISTS d_user_project_label (
        | uid int,
        | pid int,
        | rdid int,
        | label_id string,
        | mark int
        | ) STORED AS PARQUET
        |""".stripMargin
    )

    // create user label product relation table
    /*
    spark.sql(
      """
        | create table if not exists  d_user_label_product(
        | id int,
        | uid int,
        | pid int,
        | label_id int,
        | rid int,
        | rdid int
        | ) STORED AS PARQUET
        |""".stripMargin)


    val df = spark.sql("select * from d_label_detail;")
    val df1 = spark.sql("select * from d_user_label_product")
    df.show()
    df1.show()

     */
  }

  createUserTable()
}
