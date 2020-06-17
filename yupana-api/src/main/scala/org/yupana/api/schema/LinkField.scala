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

import org.yupana.api.types.DataType

/**
  * Defines metrics in some table.
  */
trait LinkField extends Serializable {

  /** Type of a value for this field */
  type T

  /** Field name */
  val name: String

  /** Value data type */
  val dataType: DataType.Aux[T]

  def aux: LinkField.Aux[T] = this

  override def toString: String = s"LinkField($name)"

  override def equals(obj: Any): Boolean = {
    if (obj == null) false
    else
      obj match {
        case that: LinkField =>
          this.name == that.name && this.dataType == that.dataType
        case _ => false
      }
  }
}

object LinkField {
  type Aux[T0] = LinkField { type T = T0 }

  def apply[T0](n: String)(implicit dt: DataType.Aux[T0]): Aux[T0] = {
    new LinkField {
      override type T = T0
      override val name: String = n
      override val dataType: DataType.Aux[T0] = dt
    }
  }
}
