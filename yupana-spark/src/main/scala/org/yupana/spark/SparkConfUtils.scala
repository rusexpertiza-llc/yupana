package org.yupana.spark

import org.apache.spark.SparkConf

object SparkConfUtils {
  def removeSparkPrefix(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(sparkConf.getAll.map { case (k, v) =>
      if (k.startsWith("spark.")) k.substring(6) -> v else k -> v
    })
  }
}
