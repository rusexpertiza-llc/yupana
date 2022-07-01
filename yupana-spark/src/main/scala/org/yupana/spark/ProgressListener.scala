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
import org.apache.spark.scheduler.{ SparkListener, SparkListenerTaskEnd }
import org.yupana.api.query.Query

abstract class ProgressListener extends SparkListener {

  def onStart(query: Query): Unit
  def onEnd(): Unit

  def onPartitionsCalculated(ps: Seq[Partition]): Unit
  def onPartitionCompleted(p: Partition): Unit

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    super.onTaskEnd(taskEnd)
    taskEnd.taskInfo.partitionId
  }
}
