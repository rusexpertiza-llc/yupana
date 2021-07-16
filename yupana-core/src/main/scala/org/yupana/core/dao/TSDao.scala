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

import scala.language.higherKinds

trait TSDao[Collection[_], IdType] extends TSReadingDao[Collection, IdType] {
  def put(mr: MapReducible[Collection], dataPoints: Collection[DataPoint], username: String): Seq[UpdateInterval] = {
    // todo we have two batchings, here in batchFlatMap (usefull) and before HTable.put (probably less usefull). Need to clarify and factor out to some setting.
    mr.materialize(mr.batchFlatMap(dataPoints, 10000)(putBatch(username)))
  }
  def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval]
}
