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

import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.{ DimOrdering, SortedSetIterator }
import org.yupana.core.TsdbBase
import org.yupana.core.utils.CollectionUtils

abstract class CrossIdBasedExternalLinkService[T <: ExternalLink](tsdb: TsdbBase)
    extends DimIdBasedExternalLinkService[T](tsdb) {

  def dimIdsForCrossJoinedValues(fieldsValues: Map[String, Any]): Seq[Long]

  override def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[Any])]): SortedSetIterator[Long] = {
    val flatValues = fieldsValues
      .groupBy(_._1)
      .map {
        case (k, v) =>
          CollectionUtils.intersectAll(v.map(_._2)).map(k -> _).toList
      }
      .toList
    val crossed = CollectionUtils.crossJoin(flatValues).map(_.toMap)
    val dimIds = crossed.flatMap { vs =>
      dimIdsForCrossJoinedValues(vs)
    }
    val ord = implicitly[DimOrdering[Long]]
    SortedSetIterator(dimIds.sortWith(ord.lt).iterator)
  }

  override def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[Any])]): SortedSetIterator[Long] = {
    val dimIds = fieldsValues.flatMap {
      case (k, vs) =>
        vs.flatMap(v => dimIdsForCrossJoinedValues(Map(k -> v)))
    }
    val ord = implicitly[DimOrdering[Long]]
    SortedSetIterator(dimIds.sortWith(ord.lt).iterator)
  }
}
