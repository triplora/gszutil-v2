package com.google.cloud.bqsh
import java.util




object GsUtilConfigF {

}
/**
  * Add an additional parameter to GsUtilConfig. filter_configs file.
  */
case class GsUtilConfigF(filter: String) extends GsUtilConfig {
  override def toMap: util.Map[String, Any] = {
    val m = super.toMap
    m.put("filter_config",String)
    m
  }
}
