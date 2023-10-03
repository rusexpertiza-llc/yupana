package org.yupana.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("yupana test")
    .getOrCreate()

  def sc: SparkContext = spark.sparkContext
}
