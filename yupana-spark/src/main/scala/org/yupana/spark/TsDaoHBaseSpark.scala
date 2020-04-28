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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.schema.Dimension
import org.yupana.core.MapReducible
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase._

class TsDaoHBaseSpark(
    @transient val sparkContext: SparkContext,
    config: Config,
    override val dictionaryProvider: DictionaryProvider
) extends TSDaoHBaseBase[RDD]
    with Serializable {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[RDD] = {
    new RddMapReducible(sparkContext, metricQueryCollector)
  }

  override def executeScans(
      queryContext: InternalQueryContext,
      from: Long,
      to: Long,
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): RDD[TSDOutputRow] = {
    if (rangeScanDims.nonEmpty) {
      val rdds = rangeScanDims.map { dimIds =>
        new HBaseScanRDD(sparkContext, config, queryContext, from, to, dimIds)
      }
      sparkContext.union(rdds.toSeq)
    } else {
      sparkContext.emptyRDD[TSDOutputRow]
    }
  }
}

object TsDaoHBaseSpark {
  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.properties)
    }
    configuration
  }
}
