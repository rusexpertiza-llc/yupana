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

package org.yupana.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.query.{ Query, QueryHint }
import org.yupana.api.schema.Schema
import org.yupana.core.dao.{ DictionaryProvider, TSDao, TsdbQueryMetricsDao }
import org.yupana.core.model.{ InternalRow, KeyData }
import org.yupana.core.utils.CloseableIterator
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector, PersistentMetricQueryReporter }
import org.yupana.core.{ ExpressionCalculatorFactory, QueryContext, TsdbBase }
import org.yupana.hbase.{ HBaseUtils, HdfsFileUtils, TsdbQueryMetricsDaoHBase }
import org.yupana.spark.TsdbSparkBase.createDefaultMetricCollector

object TsdbSparkBase extends StrictLogging {

  @transient var metricsDao: Option[TsdbQueryMetricsDao] = None

  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.settings)
    }
    configuration
  }

  def getMetricsDao(config: Config): TsdbQueryMetricsDao = metricsDao match {
    case None =>
      logger.info("TsdbQueryMetricsDao initialization...")
      val hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration(config))
      val dao = new TsdbQueryMetricsDaoHBase(hbaseConnection, config.hbaseNamespace)
      metricsDao = Some(dao)
      dao
    case Some(d) => d
  }

  private def createDefaultMetricCollector(
      config: Config,
      opName: String = "query"
  ): Query => MetricQueryCollector = { query: Query =>
    new SparkMetricCollector(
      query,
      opName,
      config.metricsUpdateInterval,
      new PersistentMetricQueryReporter(() => getMetricsDao(config), forceSaving = false)
    )
  }
}

case class ProgressHint(fileName: String) extends QueryHint

abstract class TsdbSparkBase(
    @transient val sparkContext: SparkContext,
    override val prepareQuery: Query => Query,
    conf: Config,
    override val schema: Schema
)(
    metricCollectorCreator: Query => MetricQueryCollector = createDefaultMetricCollector(conf)
) extends TsdbBase
    with Serializable {

  override type Collection[X] = RDD[X]
  override type Result = DataRowRDD

  override val extractBatchSize: Int = conf.extractBatchSize
  override val putBatchSize: Int = conf.putBatchSize

  override val calculatorFactory: ExpressionCalculatorFactory = ExpressionCalculatorFactory

  HBaseUtils.initStorage(
    ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(conf)),
    conf.hbaseNamespace,
    schema,
    conf
  )

  private val dictionaryProvider: DictionaryProvider = new SparkDictionaryProvider(conf)

  override val dao: TSDao[RDD, Long] =
    new TsDaoHBaseSpark(sparkContext, schema, conf, dictionaryProvider)

  override def createMetricCollector(query: Query): MetricQueryCollector = {
    if (conf.collectMetrics) {
      metricCollectorCreator(query)
    } else {
      NoMetricCollector
    }
  }

  override def finalizeQuery(
      queryContext: QueryContext,
      data: RDD[Array[Any]],
      metricCollector: MetricQueryCollector
  ): DataRowRDD = {
    val rdd = data.mapPartitions { it =>
      CloseableIterator(it, metricCollector.finish())
    }
    new DataRowRDD(rdd, queryContext)
  }

  def union(rdds: Seq[DataRowRDD]): DataRowRDD = {
    val rdd = sparkContext.union(rdds.map(_.rows))
    new DataRowRDD(rdd, rdds.head.queryContext)
  }

  override def applyWindowFunctions(
      queryContext: QueryContext,
      keysAndValues: RDD[(KeyData, InternalRow)]
  ): RDD[(KeyData, InternalRow)] = {
    throw new UnsupportedOperationException("Window functions are not supported in TSDB Spark")
  }
}
