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

package org.yupana.externallinks

import org.yupana.api.query.{ Condition, EmptyCondition }
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.utils.TimeBoundedCondition

trait SimpleExternalLinkConditionHandler[T <: ExternalLink] extends ExternalLinkService[T] {

  def includeCondition(values: Seq[(String, Set[String])]): Condition

  def excludeCondition(values: Seq[(String, Set[String])]): Condition

  override def condition(condition: Condition): Condition = {
    val tbcs = TimeBoundedCondition(condition)

    val r = tbcs.map { tbc =>
      val (includeValues, excludeValues, other) = ExternalLinkService.extractCatalogFields(tbc, externalLink.linkName)

      val include = if (includeValues.nonEmpty) {
        includeCondition(includeValues)
      } else {
        EmptyCondition
      }

      val exclude = if (excludeValues.nonEmpty) {
        excludeCondition(excludeValues)
      } else {
        EmptyCondition
      }

      TimeBoundedCondition(tbc.from, tbc.to, include :: exclude :: other)
    }

    TimeBoundedCondition.merge(r).toCondition
  }
}
