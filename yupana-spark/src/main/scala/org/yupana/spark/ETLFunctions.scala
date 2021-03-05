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
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Schema, Table }
import org.yupana.core.dao.RollupMetaDao

import scala.language.implicitConversions

object ETLFunctions extends StrictLogging {

  def processTransactions(
      context: EtlContext,
      schema: Schema,
      dataPoints: RDD[DataPoint],
      doInvalidateRollups: Boolean
  ): Unit = {

    dataPoints.foreachPartition { ls =>
      ls.sliding(5000, 5000).foreach { batch =>
        val dps = batch.toList

        logger.trace(s"Put ${dps.size} datapoints")
        context.tsdb.put(dps)

        val byTable = dps.groupBy(_.table)

        if (doInvalidateRollups) {
          byTable.foreach {
            case (t, ps) =>
              if (schema.rollups.exists(_.fromTable.name == t.name)) {
                markUpdatedIntervals(context.rollupMetaDao, ps, t)
              }
          }
        }
      }
    }
  }

  def markUpdatedIntervals(rollupMetaDao: RollupMetaDao, dps: Seq[DataPoint], table: Table): Unit = {
    rollupMetaDao.putUpdatesIntervals(table.name, RollupMetaDao.dataPointsToUpdatedIntervals(dps, table.rowTimeSpan))
  }

  implicit def dStream2Functions(stream: DStream[DataPoint]): DataPointStreamFunctions =
    new DataPointStreamFunctions(stream)
  implicit def rdd2Functions(rdd: RDD[DataPoint]): DataPointRddFunctions = new DataPointRddFunctions(rdd)
}

class DataPointStreamFunctions(stream: DStream[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlContext, schema: Schema, invalidateRollups: Boolean = true): DStream[DataPoint] = {
    stream.foreachRDD { rdd =>
      ETLFunctions.processTransactions(context, schema, rdd, invalidateRollups)
    }

    stream
  }
}

class DataPointRddFunctions(rdd: RDD[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlContext, schema: Schema, invalidateRollups: Boolean = false): Unit = {
    ETLFunctions.processTransactions(context, schema, rdd, invalidateRollups)
  }
}
