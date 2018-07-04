package cn.cnic.bigdata.configure.flow

import cn.cnic.bigdata.configure.bean.FlowBean
import cn.cnic.bigdata.util.FileUtil
import cn.cnic.bigdata.util.JsonUtil

import scala.util.parsing.json.JSON

object Test {

  def main(args: Array[String]): Unit = {

    val file = "src/main/resources/flow.json"
    val flowJsonStr = FileUtil.fileReader(file)
    val map = JSON.parseFull(flowJsonStr)

    FlowBean(map)

  }
}
