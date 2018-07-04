package cn.cnic.bigdata.util

import scala.util.parsing.json.JSON

object JsonUtil {

  def parse(jsonstr : String) : Map[String, String] = {
    val jsonMap : Map = JSON.parseFull(jsonstr)
    println(jsonMap)
    jsonMap
  }
}
