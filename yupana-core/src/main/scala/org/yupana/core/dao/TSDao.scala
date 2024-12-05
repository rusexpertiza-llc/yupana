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
import org.yupana.api.schema.Table
import org.yupana.core.MapReducible
import org.yupana.core.model.{ BatchDataset, UpdateInterval }

trait TSDao[Collection[_], IdType] extends TSReadingDao[Collection, IdType] {

  def put(
      mr: MapReducible[Collection],
      dataPoints: Collection[DataPoint],
      username: String
  ): Collection[UpdateInterval]

  def putDataset(
      mr: MapReducible[Collection],
      tables: Seq[Table],
      dataset: Collection[BatchDataset],
      username: String
  ): Collection[UpdateInterval]

  def putDataset(
      mr: MapReducible[Collection],
      table: Table,
      dataset: Collection[BatchDataset],
      username: String
  ): Collection[UpdateInterval] = putDataset(mr, Seq(table), dataset, username)

}
