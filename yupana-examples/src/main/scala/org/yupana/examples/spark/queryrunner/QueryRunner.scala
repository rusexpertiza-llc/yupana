/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.examples.spark.queryrunner

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
import org.yupana.examples.ExampleSchema
import org.yupana.examples.spark.TsdbSpark
import org.yupana.hbase.ChangelogDaoHBase
import org.yupana.spark.{ ResultRDD, SparkConfUtils, SparkDictionaryProvider, TsDaoHBaseSpark }

object QueryRunner {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    SparkConfUtils.removeSparkPrefix(sparkConf)

    val config = new QueryRunnerConfig(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sparkDao: TSDao[RDD, Long] =
      new TsDaoHBaseSpark(spark.sparkContext, ExampleSchema.schema, config, new SparkDictionaryProvider(config))
    val changelogDao: ChangelogDao = new ChangelogDaoHBase(
      ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(config)),
      config.hbaseNamespace
    )
    val tsdbSpark = new TsdbSpark(sparkDao, changelogDao, spark.sparkContext, identity, config, ExampleSchema.schema)

    executeQuery(config.query, tsdbSpark) match {
      case Right(rdd) =>
        rdd.toDF(spark).repartition(1).write.option("header", "true").csv(config.outPath)

      case Left(err) =>
        System.err.println(s"Failed to execute query ${config.query}, $err")
        System.exit(-1)
    }
  }

  def executeQuery(sql: String, tsdbSpark: TsdbSpark): Either[String, ResultRDD] = {
    SqlParser.parse(sql) flatMap {
      case s: Select =>
        new SqlQueryProcessor(ExampleSchema.schema).createQuery(s).map { q =>
          tsdbSpark.query(q)
        }
      case _ => Left(s"Unsupported query: $sql")
    }

  }
}
