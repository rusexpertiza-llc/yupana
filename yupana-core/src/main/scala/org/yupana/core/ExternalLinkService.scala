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

package org.yupana.core

import org.yupana.api.query._
import org.yupana.api.schema.{ ExternalLink, Schema }
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.FlatAndCondition

trait ExternalLinkService[T <: ExternalLink] {

  def externalLink: T

  def schema: Schema

  val putEnabled: Boolean = false

  /**
    * Sets requested external link expressions values into a batch of ValueData
    *
    * @param rows batch of rows to be updated
    * @param exprs expressions to be set
    */
  def setLinkedValues(
      dstaset: BatchDataset,
      exprs: Set[LinkExpr[_]]
  ): Unit

  /**
    * Transforms condition according to filter by this external link conditions.
    * For example:
    *
    * {{{
    *   linkX_fieldY = 'value1' AND
    *     linkY_fieldZ = 'value2' AND
    *     time >= TIMESTAMP '2019-02-01' AND
    *     time < TIMESTAMP '2019-02-28'
    * }}}
    *
    * might be transformed by `ExternalLinkService` for catalogX to:
    *
    * {{{
    *   tagX in (`tagValue1`, `tagValue2`) AND
    *     linkY_fieldZ = 'value2' AND
    *     time >= TIMESTAMP '2019-02-01' AND
    *     time < TIMESTAMP '2019-02-28'
    * }}}
    *
    * So, the main idea is to provide more primitive conditions using values for supported fields (and maybe other fields
    * as a context for catalog).
    *
    * @param condition condition to be transformed
    * @return sequence of transformations applied to the initial condition, basically each transformation is a mapping from one expression to another. It should preserve time bounds even if there no conditions supported by this catalog.
    */
  def transformCondition(condition: FlatAndCondition): Seq[ConditionTransformation]

  def put(dataPoints: Seq[DataPoint]): Unit

  def put(batchDataset: BatchDataset): Unit
}
