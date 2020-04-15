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

import java.nio.ByteBuffer

import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, MetricValue, Table }

object TSDRowValues {

  type TimeShiftedValue = (Long, Array[Byte])
  type TimeShiftedValues = Array[TimeShiftedValue]
  type ValuesByGroup = Map[Int, TimeShiftedValues]

  def valuesByGroup(table: Table, dataPoints: Seq[DataPoint]): ValuesByGroup = {
    dataPoints.map(partitionValuesByGroup(table)).reduce(mergeMaps).mapValues(_.toArray)
  }

  private def partitionValuesByGroup(table: Table)(dp: DataPoint): Map[Int, Seq[TimeShiftedValue]] = {
    val timeShift = HBaseUtils.restTime(dp.time, table)
    dp.metrics
      .groupBy(_.metric.group)
      .mapValues(metricValues => Seq((timeShift, fieldsToBytes(table, dp.dimensions, metricValues))))
  }

  private def mergeMaps(
      m1: Map[Int, Seq[TimeShiftedValue]],
      m2: Map[Int, Seq[TimeShiftedValue]]
  ): Map[Int, Seq[TimeShiftedValue]] = {
    (m1.keySet ++ m2.keySet).map(k => (k, m1.getOrElse(k, Seq.empty) ++ m2.getOrElse(k, Seq.empty))).toMap
  }

  private def fieldsToBytes(
      table: Table,
      dimensions: Map[Dimension, Any],
      metricValues: Seq[MetricValue]
  ): Array[Byte] = {
    val metricFieldBytes = metricValues.map { f =>
      val bytes = f.metric.dataType.storable.write(f.value)
      (f.metric.tag, bytes)
    }
    val dimensionFieldBytes = dimensions.collect {
      case (d, value) if table.dimensionTagExists(d) =>
        val tag = table.dimensionTag(d)
        val bytes = d.dataType.storable.write(value.asInstanceOf[d.T])
        (tag, bytes)
    }

    val fieldBytes = metricFieldBytes ++ dimensionFieldBytes

    val size = fieldBytes.map(_._2.length).sum + fieldBytes.size
    val bb = ByteBuffer.allocate(size)
    fieldBytes.foreach {
      case (tag, bytes) =>
        bb.put(tag)
        bb.put(bytes)
    }

    bb.array()
  }
}
