package cn.cnic.bigdata.bundle.common

import cn.cnic.bigdata.bundle.ConfigurableStop
import cn.piflow.lib.DoMerge
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}

class Merge extends ConfigurableStop{

  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    out.write(in.ports().map(in.read(_)).reduce((x, y) => x.union(y)));
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {

  }
}
