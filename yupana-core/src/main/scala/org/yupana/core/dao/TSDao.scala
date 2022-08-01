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

package org.yupana.core.dao

import org.yupana.api.query.DataPoint
import org.yupana.core.MapReducible
import org.yupana.core.model.UpdateInterval

trait TSDao[Collection[_]] extends TSReadingDao[Collection] {

  val dataPointsBatchSize: Int

  def put(mr: MapReducible[Collection], dataPoints: Collection[DataPoint], username: String): Seq[UpdateInterval] = {
    val updateIntervalsCollection = mr.batchFlatMap(dataPoints, dataPointsBatchSize)(putBatch(username))
    val updateIntervalsByWhatUpdated = mr.map(updateIntervalsCollection)(i => i.whatUpdated -> i)
    val mostRecentUpdateIntervalsByWhatUpdated =
      mr.reduceByKey(updateIntervalsByWhatUpdated)((i1, i2) => if (i1.updatedAt.isAfter(i2.updatedAt)) i1 else i2)
    val mostRecentUpdateIntervals = mr.map(mostRecentUpdateIntervalsByWhatUpdated)(_._2)
    mr.materialize(mostRecentUpdateIntervals).distinct
  }

  def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval]
}
