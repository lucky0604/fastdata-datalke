package com.labelhub.datalake.utils

import com.labelhub.datalake.utils.FilesOperation.del_dir
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

import java.io.{FileSystem => _, _}
import java.io.File

object ReadCSVToHDFS extends App {

  def getFiles(file: File): Array[File] = {
    val files = file.listFiles().filter(! _.isDirectory)
      .filter(t => t.toString.endsWith(".csv"))
    files ++ file.listFiles().filter(_.isDirectory).flatMap(getFiles)
  }

  def copyFileToHDFS(hdfs: FileSystem, localSource: String, hdfsTarget: String) = {
    val targetPath = new Path(hdfsTarget)
    if (!hdfsExists(hdfs, targetPath)) {
      hdfsCreateFile(hdfs, targetPath)
    }
    val inputStream: FileInputStream = new FileInputStream(localSource)
    val outputStream: FSDataOutputStream = hdfs.create(targetPath)
    transport(inputStream, outputStream)
  }

  def hdfsExists(hdfs: FileSystem, name: Path): Boolean = {
    hdfs.exists(name)
  }

  def hdfsCreateFile(hdfs: FileSystem, name: Path): Boolean = {
    hdfs.createNewFile(name)
  }

  def transport(inputStream: InputStream, outputStream: OutputStream) = {
    val buffer = new Array[Byte](64 * 1000)
    var len = inputStream.read(buffer)
    while (len != -1) {
      outputStream.write(buffer, 0, len - 1)
      len = inputStream.read(buffer)
    }
    outputStream.flush()
    inputStream.close()
    outputStream.close()
  }

  val sparkSession = SparkSession.builder()
    .appName("Put File Into HDFS")
    .master("spark://localhost:7077")
    .getOrCreate()
  val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
  val hdfs = FileSystem.get(hadoopConf)

  val csvPath = Thread.currentThread().getContextClassLoader.getResource("csv_folder").getPath
  val file = new File(csvPath)
  getFiles(file).foreach(item => {
    copyFileToHDFS(hdfs, item.getAbsolutePath, "/user/hive/warehouse/csv/" + item.getName)
  })
}
