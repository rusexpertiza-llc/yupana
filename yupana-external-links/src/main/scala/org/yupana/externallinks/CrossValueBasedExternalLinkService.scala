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
import org.yupana.core.TsdbBase
import org.yupana.core.utils.CollectionUtils

abstract class CrossValueBasedExternalLinkService[T <: ExternalLink](tsdb: TsdbBase)
    extends DimValueBasedExternalLinkService[T](tsdb) {

  def tagValuesForCrossJoinedValues(fieldsValues: Map[String, Any]): Seq[String]

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    val flatValues = fieldsValues
      .groupBy(_._1)
      .map {
        case (k, vs) =>
          CollectionUtils.intersectAll(vs.map(_._2)).toList.map(k -> _)
      }
      .toList

    val crossed = CollectionUtils.crossJoin(flatValues).map(_.toMap)

    crossed.flatMap { vs =>
      tagValuesForCrossJoinedValues(vs)
    }.toSet
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    fieldsValues.flatMap {
      case (k, vs) =>
        vs.flatMap(v => tagValuesForCrossJoinedValues(Map(k -> v)))
    }.toSet
  }
}
