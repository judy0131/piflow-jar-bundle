package cn.cnic.bigdata.configure.bean

import cn.cnic.bigdata.util.OptionUtil

class StopBean {

  var uuid : String = _
  var name : String = _
  var bundle : String = _
  var properties : Map[String, String] = _

  def init(uuid:String, name:String, bundle:String, properties:Map[String,String]) = {
    this.uuid = uuid
    this.name = name
    this.bundle = bundle
    this.properties = properties
  }

  def init(map:Map[String,String]) = {
    this.uuid = OptionUtil.get(map.get("uuid"))
    this.name = OptionUtil.get(map.get("name"))
    this.bundle = OptionUtil.get(map.get("bundle"))
    this.properties = OptionUtil.get(map.get("uuid"))
  }

}
