package cn.cnic.bigdata.util

object OptionUtil {

  def get(x: Option[String]) : String = {
    x match {
      case Some(x)  => x
      case None => throw new IllegalArgumentException
    }
  }

}
