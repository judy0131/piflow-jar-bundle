package cn.cnic.bigdata.configure.bean

import cn.cnic.bigdata.util.MapUtil

class PathBean {

  var from : String = _
  var to : String = _

  def init(from:String, to:String)= {
    this.from = from
    this.to = to
  }
  def init(map:Map[String,Any])= {
    this.from = MapUtil.get(map,"from").asInstanceOf[String]
    this.to = MapUtil.get(map,"to").asInstanceOf[String]
  }

}

object PathBean{
  def apply(map : Map[String, Any]): PathBean = {
    val pathBean = new PathBean()
    pathBean.init(map)
    pathBean
  }
}
