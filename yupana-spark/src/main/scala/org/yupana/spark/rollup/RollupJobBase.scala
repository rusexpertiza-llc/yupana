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

package org.yupana.spark.rollup

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.joda.time.Interval
import org.yupana.api.Time
import org.yupana.api.query.{ AndExpr, DataPoint, Query, QueryField }
import org.yupana.api.schema.{ Metric, MetricValue, Rollup, Table }
import org.yupana.core.dao.TSDao
import org.yupana.spark.{ DataRowRDD, TsdbSparkBase }

class RollupJobBase extends StrictLogging {
  def queryFromSchema(
      tsdbSpark: TsdbSparkBase,
      rollup: Rollup,
      fromSchemaFields: Seq[QueryField],
      recalcIntervals: Seq[Interval]
  ): DataRowRDD = {

    import org.yupana.api.query.syntax.All._

    val baseQuery = Query(
      table = Some(rollup.fromTable),
      fields = fromSchemaFields,
      filter = None,
      groupBy = rollup.allGroupBy
    )
    val inputDataRDDs = recalcIntervals.map(interval =>
      tsdbSpark.query(
        baseQuery.copy(filter =
          Some(
            AndExpr(
              Seq(
                ge(time, const(Time(interval.getStart))),
                lt(time, const(Time(interval.getEnd)))
              ) ++ rollup.filter
            )
          )
        )
      )
    )

    tsdbSpark.union(inputDataRDDs)
  }

  def doRollup(tsdbSpark: TsdbSparkBase, rollup: Rollup, recalcIntervals: Seq[Interval]): Unit = {
    logger.debug(s"Performing rollup from ${rollup.fromTable.name} to ${rollup.toTable.name}")
    val fromSchemaFields = rollup.allFields.map(_.queryField)
    val rawData = queryFromSchema(tsdbSpark, rollup, fromSchemaFields, recalcIntervals)
    val aggregatedData = createAggregatedDataPointsRDD(rawData, rollup)
    tsdbSpark.writeRDD(aggregatedData, rollup.toTable)
  }

  def tryMarkEverythingValid(
      validityMap: Map[Long, String],
      localTsdDao: TSDao[Iterator, Long],
      base: Table
  ): Unit = {
    validityMap.foreach {
      case (time, invalidMark) => localTsdDao.checkAndPutRollupStatus(time, Some(invalidMark), "", base)
    }
  }

  private def createAggregatedDataPointsRDD(
      fieldValuesRDD: DataRowRDD,
      rollup: Rollup
  ): RDD[DataPoint] = {

    fieldValuesRDD.map { row =>
      val tags = rollup.toTable.dimensionSeq.flatMap { dimension =>
        val value = row.fieldValueByName[dimension.T](rollup.getResultFieldForDimName(dimension.name))
        value.map(v => dimension -> v)
      }.toMap

      val values = rollup.toTable.metrics.flatMap { metric =>
        val value = row.fieldValueByName[metric.T](rollup.getResultFieldForMeasureName(metric.name))
        value.map(v => MetricValue[metric.T](metric.asInstanceOf[Metric.Aux[metric.T]], v))
      }

      val time = row.fieldValueByName[Time](Table.TIME_FIELD_NAME).get
      DataPoint(rollup.toTable, time.millis, tags, values)
    }
  }

}
