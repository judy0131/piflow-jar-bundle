package cn.cnic.bigdata.configure.bean

import cn.cnic.bigdata.util.{MapUtil, OptionUtil}
import cn.piflow.{Flow, FlowImpl, Path}

class FlowBean {
  var uuid : String = _
  var name : String = _
  var stops : List[StopBean] = List()
  var paths : List[PathBean] = List()

  def init(map : Map[String, Any]) = {

    val flowMap = MapUtil.get(map, "flow").asInstanceOf[Map[String, Any]]

    this.uuid = MapUtil.get(flowMap,"uuid").asInstanceOf[String]
    this.name = MapUtil.get(flowMap,"name").asInstanceOf[String]

    //construct StopBean List
    val stopsList = MapUtil.get(flowMap,"stops").asInstanceOf[List[Map[String, Any]]]
    stopsList.foreach( stopMap => {
      val stop = StopBean(stopMap.asInstanceOf[Map[String, Any]])
      this.stops =   stop +: this.stops
    })

    //construct PathBean List
    val pathsList = MapUtil.get(flowMap,"paths").asInstanceOf[List[Map[String, Any]]]
    pathsList.foreach( pathMap => {
      val path = PathBean(pathMap.asInstanceOf[Map[String, Any]])
      this.paths = path +: this.paths
    })

  }

  //create Flow by FlowBean
  def constructFlow()= {
    val flow = new FlowImpl();
    this.stops.foreach( stopBean => {
      flow.addStop(stopBean.name,stopBean.constructStop())
    })
    this.paths.foreach( pathBean => {
      flow.addPath(Path.from(pathBean.from).via(pathBean.outport, pathBean.inport).to(pathBean.to))
    })

    flow
  }

}

object FlowBean{
  def apply(map : Map[String, Any]): FlowBean = {
    val flowBean = new FlowBean()
    flowBean.init(map)
    flowBean
  }

}
