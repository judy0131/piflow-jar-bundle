package cn.cnic.bigdata.bundle

import cn.piflow.Stop

trait ConfigurableStop extends Stop{

  def setProperties(map: Map[String, Any])

}
