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

import org.joda.time.Interval
import org.yupana.api.schema.Table
import org.yupana.core.model.UpdateInterval

trait RollupMetaDao {
  def putUpdatesIntervals(tableName: String, periods: Seq[UpdateInterval]): Unit
  def getUpdatesIntervals(tableName: String, rollupIntervalOpt: Interval): Iterable[UpdateInterval]

  def getRollupSpecialField(fieldName: String, table: Table): Option[Long]
  def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit

  def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)]
  def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit
  def checkAndPutRollupStatus(time: Long, oldStatus: Option[String], newStatus: String, table: Table): Boolean
}
