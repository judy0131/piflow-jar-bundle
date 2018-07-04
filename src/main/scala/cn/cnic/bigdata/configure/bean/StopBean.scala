package cn.cnic.bigdata.configure.bean

import cn.cnic.bigdata.util.{MapUtil, OptionUtil}
import cn.piflow.Stop

class StopBean {

  var uuid : String = _
  var name : String = _
  var bundle : String = _
  var properties : Map[String, String] = _

  def init(map:Map[String,Any]) = {
    this.uuid = MapUtil.get(map,"uuid").asInstanceOf[String]
    this.name = MapUtil.get(map,"name").asInstanceOf[String]
    this.bundle = MapUtil.get(map,"bundle").asInstanceOf[String]
    this.properties = MapUtil.get(map, "properties").asInstanceOf[Map[String, String]]
  }

  def constructStop() : Stop = {
    val stop = Class.forName(this.bundle).getConstructor(classOf[Map[String, String]]).newInstance(this.properties)
    stop.asInstanceOf[Stop]
  }

}

object StopBean  {

  def apply(map : Map[String, Any]): StopBean = {
    val stopBean = new StopBean()
    stopBean.init(map)
    stopBean
    //stopBean.getStop()
  }

}
