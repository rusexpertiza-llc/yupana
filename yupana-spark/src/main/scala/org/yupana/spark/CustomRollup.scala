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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.threeten.extra.Interval
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Expression }
import org.yupana.api.schema.{ Metric, MetricValue, Rollup, Table }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }

abstract class CustomRollup(
    override val name: String,
    override val timeExpr: Expression[Time],
    override val fromTable: Table,
    override val toTable: Table
) extends Rollup
    with Serializable {

  protected val sqlQueryProcessor: SqlQueryProcessor

  def doRollup(tsdbSpark: TsdbSparkBase, recalcIntervals: Seq[Interval]): RDD[DataPoint]

  protected def toDataPoints(rdd: RDD[Row]): RDD[DataPoint] = {
    rdd.map { row =>
      val dimensions = toTable.dimensionSeq.map { dimension =>
        val value = row.getAs[dimension.T](dimension.name)
        dimension -> value
      }.toMap

      val metrics = toTable.metrics.flatMap { metric =>
        val value = getOpt[metric.T](row, metric.name)
        value.map(v => MetricValue[metric.T](metric.asInstanceOf[Metric.Aux[metric.T]], v))
      }

      val timeMillis = row.getAs[Long](Table.TIME_FIELD_NAME)
      DataPoint(toTable, timeMillis, dimensions, metrics)
    }
  }

  protected def executeQuery(tsdbSpark: TsdbSparkBase, sql: String): tsdbSpark.Result = {
    SqlParser.parse(sql) flatMap {
      case s: Select => sqlQueryProcessor.createQuery(s)
      case _         => Left(s"Bad query ($sql), Select expected")
    } match {
      case Right(query) =>
        tsdbSpark.query(query)
      case Left(msg) =>
        throw new RuntimeException(s"Bad query ($sql): $msg")
    }
  }

  private def getOpt[T](row: Row, fieldName: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(fieldName))) None
    else Some(row.getAs[T](fieldName))
  }
}
