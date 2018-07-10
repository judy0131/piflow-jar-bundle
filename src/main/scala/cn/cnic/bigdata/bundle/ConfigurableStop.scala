package cn.cnic.bigdata.bundle

import cn.cnic.bigdata.configure.bean.PropertyDescriptor
import cn.piflow.Stop


trait ConfigurableStop extends Stop{

  def setProperties(map: Map[String, Any])

  def getPropertyDescriptor() : List[PropertyDescriptor]

}
