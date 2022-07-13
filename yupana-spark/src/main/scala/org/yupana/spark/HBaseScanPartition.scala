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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partition
import org.yupana.api.schema.Dimension
import org.yupana.hbase.InternalQueryContext

case class HBaseScanPartition(
    override val index: Int,
    startKey: Array[Byte],
    endKey: Array[Byte],
    fromTime: Long,
    toTime: Long,
    queryContext: InternalQueryContext,
    rangeScanDimsIds: Map[Dimension, Seq[_]]
) extends Partition

object HBaseScanPartition {

  class HBaseScanPartitionStorable(
      fromTime: Long,
      toTime: Long,
      queryContext: InternalQueryContext,
      rangeScanDims: Map[Dimension, Seq[_]]
  ) extends PartitionStorable[HBaseScanPartition]
      with Serializable {
    override def asString(p: HBaseScanPartition): String = s"${Bytes.toHex(p.startKey)}-${Bytes.toHex(p.endKey)}"

    override def fromString(s: String, index: Int): HBaseScanPartition = {
      val Array(start, end) = s.split("-", -1): @unchecked
      new HBaseScanPartition(
        index,
        Bytes.fromHex(start),
        Bytes.fromHex(end),
        fromTime,
        toTime,
        queryContext,
        rangeScanDims
      )
    }
  }
}
