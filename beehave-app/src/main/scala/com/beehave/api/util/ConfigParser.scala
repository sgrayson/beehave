package com.beehave.api.util

import java.io.{BufferedReader, File, FileReader}

import scala.collection.mutable.Map

class ConfigParser {

  val Path = "/etc/keys/"

  lazy val configMap: Map[String, Map[String,String]] = {
    val map = Map[String, Map[String, String]]()
    val files = new File(Path)
      .listFiles
      .filter(_.getName.endsWith(".yml"))
      .map { file =>
        val reader = new BufferedReader(new FileReader(file))
        val fileBaseName = file.getName.substring(0, file.getName.lastIndexOf("."))
        var line = reader.readLine
        while (line != null) {
          var keyMap = map.get(fileBaseName).getOrElse(Map())
          val values = line.split("=")
          keyMap.put(values(0), values(1))
          map.put(fileBaseName, keyMap)
          line = reader.readLine
        }
      }
    map
  }
}
