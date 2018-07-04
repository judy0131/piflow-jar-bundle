package cn.cnic.bigdata.configure.bean

import cn.cnic.bigdata.bundle.hive.{PutHiveStreaming, SelectHiveQL}
import cn.cnic.bigdata.util.OptionUtil
import cn.piflow.{Flow, FlowImpl, Path}

class FlowBean {
  var uuid : String = _
  var name : String = _
  var stops : List[StopBean] = _
  var paths : List[PathBean] = _

  def init(map : Map[String, String]) : FlowBean = {
    this.uuid = OptionUtil.get(map.get("uuid"))
    this.name = OptionUtil.get(map.get("name"))
    val stopsList = OptionUtil.get(map.get("stops"))
    stopsList.foreach( stopsMap => {
      val stopBean = new StopBean
      stopBean.init(stopsMap.asInstanceOf[Map[String,String]])

    })
  }
  def getFlowImpl()= {

  }

}

object FlowBean{
  def apply(map : Map[String, String]): FlowBean = {
    val flowBean = new FlowBean()
    flowBean.init(map)
    flowBean
  }

}
