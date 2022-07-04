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

import org.apache.spark.Partition

import java.util.Properties

class RddProgressListenerImpl[P <: Partition](
    fileName: String,
    storable: PartitionStorable[P],
    properties: Properties
) extends RddProgressListener[P] {

  private var partitions: Map[Int, P] = Map.empty
  private val progressSaver: ProgressSaver[P] = new HDFSProgressSaver[P](fileName, storable, properties)

  override def transformPartitions(ps: Seq[P]): Seq[P] = {
    val existing = progressSaver.readPartitions
    val result = if (existing.nonEmpty) {
      existing
    } else {
      progressSaver.writePartitions(ps)
      ps
    }
    partitions = result.map(p => p.index -> p).toMap
    result
  }

  override def onPartitionCompleted(partitionIndex: Int): Unit = {
    progressSaver.writeProgress(partitions(partitionIndex))
  }
}
