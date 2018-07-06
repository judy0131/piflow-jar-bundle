package cn.cnic.bigdata.util

object OptionUtil {

  def get(x: Option[String]) : String = {
    x match {
      case Some(x)  => x
      case None => throw new IllegalArgumentException
    }
  }

  def getAny(x: Option[Any]) : Any = {
    x match {
      case Some(x)  => x
      case None => throw new IllegalArgumentException
    }
  }

  def getOrElse(x: Option[String], default : String) : String = {
    x match {
      case Some(x)  => x
      case None => default
    }
  }

}
