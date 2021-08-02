package com.labelhub.datalake.utils

import java.io.File


object FilesOperation extends App {

  def del_dir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        del_dir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()

  }

  def remove_folders(path: String, foldername: String): Unit = {

    val rootdir: File = new File(path)
    val children: Array[File] = rootdir.listFiles.filter(_.isDirectory())
    children.map(i => {
      val subdir = i.listFiles()
      for (item <- subdir) {
        if (item.isDirectory) {
          del_dir(item)
        } else if (item.isFile) {
          if (item.getName.contains("csv")) {
            println(item.getName)
          }
        }
      }
    })
  }

  val proPath = Thread.currentThread().getContextClassLoader.getResource("csv_folder").getPath
  println(proPath)
  remove_folders(proPath, "")
}
