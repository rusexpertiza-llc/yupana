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

package org.yupana.core.sql.parser

import org.threeten.extra.PeriodDuration
import org.yupana.api.types.DataType

sealed trait Value {
  def asString: String
}

sealed trait Literal extends Value

case class Placeholder(id: Int) extends Value {
  override def asString: String = s"param#$id"
}

case class TypedValue[T](value: T)(implicit val dataType: DataType.Aux[T]) extends Literal {
  override def asString: String = value.toString
}

case class PeriodValue(value: PeriodDuration) extends Literal {
  override def asString: String = value.toString
}

case class TupleValue(a: Value, b: Value) extends Literal {
  override def asString: String = s"${a.asString}_${b.asString}"
}

case object NullValue extends Literal {
  override def asString: String = "null"
}
