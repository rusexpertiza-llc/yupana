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

package org.yupana.api.types

trait Num[N] extends Serializable {

  def negate(a: N): N
  def abs(a: N): N

  def toDouble(a: N): Double
}

object Num {
  implicit def fromNumeric[N](implicit n: Numeric[N]): Num[N] = new Num[N] {
    override def negate(a: N): N = n.negate(a)

    override def abs(a: N): N = n.abs(a)

    override def toDouble(a: N): Double = n.toDouble(a)
  }
}
