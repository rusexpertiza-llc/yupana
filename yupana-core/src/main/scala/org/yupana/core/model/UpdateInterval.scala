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

package org.yupana.core.model

import org.threeten.extra.Interval

import java.time.OffsetDateTime

case class UpdateInterval(
    table: String,
    from: OffsetDateTime,
    to: OffsetDateTime,
    updatedAt: OffsetDateTime,
    updatedBy: String
) {

  lazy val interval: Interval = Interval.of(from.toInstant, to.toInstant)
}

object UpdateInterval {
  val updatedAtColumn = "updated_at"
  val updatedByColumn = "updated_by"
  val fromColumn = "from"
  val toColumn = "to"
  val tableColumn = "table"
}
