package org.yupana.spark

import org.apache.spark.SparkConf
import org.yupana.core.settings.Settings

import scala.util.Try

case class SparkConfSettings(sc: SparkConf) extends Settings with Serializable {
  override def getByKey(k: String): Option[String] = Try(sc.get(k)).toOption
}
