package cn.cnic.bigdata.configure.bean

class PathBean {

  var from : String = _
  var to : String = _

  def init(from:String, to:String)= {
    this.from = from
    this.to = to
  }

}
