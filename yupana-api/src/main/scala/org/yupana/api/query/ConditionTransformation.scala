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

package org.yupana.api.query

/**
  * Condition transformation. Used in external links to convert conditions in terms of external link fields to conditions
  * in terms of basic schema fields (dimensions, metrics, time).
  */
sealed trait ConditionTransformation

/**
  * Add new condition transformation
  * @param c condition to be added
  */
case class AddCondition(c: SimpleCondition) extends ConditionTransformation

/**
  * Remove existing condition transformation
  * @param c condition to be removed
  */
case class RemoveCondition(c: SimpleCondition) extends ConditionTransformation

object ConditionTransformation {
  def replace(in: Seq[SimpleCondition], out: Seq[SimpleCondition]): Seq[ConditionTransformation] = {
    in.map(RemoveCondition.apply) ++ out.map(AddCondition.apply)
  }

  def replace(in: Seq[SimpleCondition], out: SimpleCondition): Seq[ConditionTransformation] = {
    in.map(RemoveCondition.apply) :+ AddCondition(out)
  }
}
