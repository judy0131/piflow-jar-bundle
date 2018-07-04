package cn.cnic.bigdata.configure.flow

import cn.cnic.bigdata.configure.bean.FlowBean
import cn.cnic.bigdata.util.FileUtil
import cn.cnic.bigdata.util.MapUtil

import scala.util.parsing.json.JSON

object Test {

  def main(args: Array[String]): Unit = {

    val file = "src/main/resources/flow.json"
    val flowJsonStr = FileUtil.fileReader(file)
    val map = JSON.parseFull(flowJsonStr).asInstanceOf[Map[String, Any]]

    val flowBean = FlowBean(map)
    val flowImpl = flowBean.constructFlow()

  }
}
