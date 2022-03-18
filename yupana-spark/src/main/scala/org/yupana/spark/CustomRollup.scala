package org.yupana.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.threeten.extra.Interval
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Expression }
import org.yupana.api.schema.{ Metric, MetricValue, Rollup, Table }

abstract class CustomRollup(
    override val name: String,
    override val timeExpr: Expression[Time],
    override val toTable: Table
) extends Rollup
    with Serializable {

  def doRollup(tsdbSpark: TsdbSparkBase, recalcIntervals: Seq[Interval]): RDD[DataPoint]

  protected def toDataPoints(rdd: RDD[Row], time: Long): RDD[DataPoint] = {
    rdd.map { row =>
      val dimensions = toTable.dimensionSeq.map { dimension =>
        val value = row.getAs[dimension.T](dimension.name)
        dimension -> value
      }.toMap

      val metrics = toTable.metrics.flatMap { metric =>
        val value = getOpt[metric.T](row, metric.name)
        value.map(v => MetricValue[metric.T](metric.asInstanceOf[Metric.Aux[metric.T]], v))
      }

      DataPoint(toTable, time, dimensions, metrics)
    }
  }

  private def getOpt[T](row: Row, fieldName: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(fieldName))) None
    else Some(row.getAs[T](fieldName))
  }
}
