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

package org.yupana.hbase

import org.apache.hadoop.hbase.client.{ Connection, Result => HResult }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Schema }
import org.yupana.api.utils.ResourceUtils._
import org.yupana.core.MapReducible
import org.yupana.core.dao.{ DictionaryProvider, TSDao }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase.HBaseUtils._

import scala.collection.JavaConverters._

class TSDaoHBase(
    override val schema: Schema,
    connection: Connection,
    namespace: String,
    override val dictionaryProvider: DictionaryProvider,
    putsBatchSize: Int = 1000
) extends TSDaoHBaseBase[Iterator]
    with TSDao[Iterator, Long] {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
    MapReducible.iteratorMR

  override def executeScans(
      queryContext: InternalQueryContext,
      from: IdType,
      to: IdType,
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): Iterator[HResult] = {

    if (rangeScanDims.nonEmpty) {
      rangeScanDims.flatMap { dimIds =>
        queryContext.metricsCollector.createScans.measure(1) {
          val filter = multiRowRangeFilter(queryContext.table, from, to, dimIds)
          createScan(queryContext, filter, Seq.empty, from, to)
        } match {
          case Some(scan) => executeScan(connection, namespace, scan, queryContext, EXTRACT_BATCH_SIZE)
          case None       => Iterator.empty
        }
      }
    } else {
      Iterator.empty
    }
  }

  override def put(dataPoints: Seq[DataPoint]): Unit = {
    logger.trace(s"Put ${dataPoints.size} dataPoints to tsdb")
    logger.trace(s" -- DETAIL DATAPOINTS: \r\n ${dataPoints.mkString("\r\n")}")

    createPuts(dataPoints, dictionaryProvider).foreach {
      case (table, puts) =>
        using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
          puts
            .sliding(putsBatchSize, putsBatchSize)
            .foreach(putsBatch => hbaseTable.put(putsBatch.asJava))
          logger.trace(s" -- DETAIL ROWS IN TABLE ${table.name}: ${puts.length}")
        }
    }
  }

}
