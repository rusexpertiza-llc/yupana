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

package org.yupana.api.schema

/**
  * Defines external data source, which are linking to [[Table]]s.  Usually external links data mapped as one to
  * many to one of the table dimensions.  For example if you have person as a dimension, external link may contain info
  * about address, so you can query data by city.
  */
trait ExternalLink extends Serializable {

  /** Name of this external link */
  val linkName: String

  type DimType

  /** Attached dimension */
  val dimension: Dimension.Aux[DimType]

  /** Set of field names for this link */
  val fields: Set[LinkField]

  override def hashCode(): Int = linkName.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case that: ExternalLink => this.linkName == that.linkName
    case _                  => false
  }

  override def toString: String = s"ExternalLink($linkName)"
}

object ExternalLink {
  type Aux[T] = ExternalLink { type DimType = T }
}
