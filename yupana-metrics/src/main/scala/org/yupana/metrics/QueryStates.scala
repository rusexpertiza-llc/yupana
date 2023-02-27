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

package org.yupana.metrics

object QueryStates {
  sealed abstract class QueryState(val name: String) {
    override def toString: String = name
  }

  case object Running extends QueryState("RUNNING")

  case object Finished extends QueryState("FINISHED")

  case object Cancelled extends QueryState("CANCELLED")
  def combine(a: QueryState, b: QueryState): QueryState = {
    if (a == b) a
    else if (a == Cancelled || b == Cancelled) Cancelled
    else if (a == Finished || b == Finished) Finished
    else Running
  }

  private val states: List[QueryState] = List(Running, Finished, Cancelled)

  def getByName(name: String): QueryState =
    states.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"State with name $name not found"))
}
