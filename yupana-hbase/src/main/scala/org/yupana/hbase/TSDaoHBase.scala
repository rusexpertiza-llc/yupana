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
import org.yupana.api.schema.{ Dimension, Schema, Table }
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.model.{ BatchDataset, UpdateInterval }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.{ IteratorMapReducible, MapReducible }
import org.yupana.hbase.HBaseUtils._

class TSDaoHBase(
    override val schema: Schema,
    connection: Connection,
    namespace: String,
    override val dictionaryProvider: DictionaryProvider,
    putsBatchSize: Int = TSDaoHBaseBase.PUTS_BATCH_SIZE,
    reduceLimit: Int
) extends TSDaoHBaseBase[Iterator] {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
    new IteratorMapReducible(reduceLimit)

  override def executeScans(
      queryContext: InternalQueryContext,
      intervals: Seq[(Long, Long)],
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): Iterator[HResult] = {

    val totalFrom = intervals.map(_._1).min
    val totalTo = intervals.map(_._2).max

    if (rangeScanDims.nonEmpty) {
      rangeScanDims.flatMap { dimIds =>
        val filter = multiRowRangeFilter(queryContext.table, intervals, dimIds)
        createScan(queryContext, filter, Seq.empty, totalFrom, totalTo) match {
          case Some(scan) => executeScan(connection, namespace, scan, queryContext, TSDaoHBaseBase.EXTRACT_BATCH_SIZE)
          case None       => Iterator.empty
        }
      }
    } else {
      Iterator.empty
    }
  }

  override def put(
      mr: MapReducible[Iterator],
      dataPoints: Iterator[DataPoint],
      username: String
  ): Iterator[UpdateInterval] = {
    mr.batchFlatMap(dataPoints, putsBatchSize) { dataPointsBatch =>
      doPutBatch(connection, dictionaryProvider, namespace, username, dataPointsBatch)
    }
  }

  override def putDataset(
      mr: MapReducible[Iterator],
      tables: Seq[Table],
      dataset: Iterator[BatchDataset],
      username: String
  ): Iterator[UpdateInterval] = {
    mr.flatMap(dataset) { batch =>
      tables.flatMap(table => doPutBatchDataset(connection, dictionaryProvider, namespace, username, batch, table))
    }
  }
}
