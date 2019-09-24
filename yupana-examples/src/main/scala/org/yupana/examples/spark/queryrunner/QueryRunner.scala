package org.yupana.examples.spark.queryrunner

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{Select, SqlParser}
import org.yupana.examples.ExampleSchema
import org.yupana.examples.spark.TsdbSpark
import org.yupana.spark.{DataRowRDD, SparkConfUtils}

object QueryRunner {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    SparkConfUtils.removeSparkPrefix(sparkConf)

    val config = new QueryRunnerConfig(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val tsdbSpark = new TsdbSpark(spark.sparkContext, identity, config, ExampleSchema.schema)

    executeQuery(config.query, tsdbSpark) match {
      case Right(rdd) =>
        rdd.toDF(spark).repartition(1).write.option("header", "true").csv(config.outPath)

      case Left(err) =>
        System.err.println(s"Failed to execute query ${config.query}, $err")
        System.exit(-1)
    }
  }

  def executeQuery(sql: String, tsdbSpark: TsdbSpark): Either[String, DataRowRDD] = {
    new SqlParser().parse(sql).right flatMap {
      case s: Select =>
        new SqlQueryProcessor(ExampleSchema.schema).createQuery(s).right.map { q =>
          tsdbSpark.query(q)
        }
      case _ => Left(s"Unsupported query: $sql")
    }

  }
}
