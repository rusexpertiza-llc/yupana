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

package org.yupana.protocol

sealed trait ParameterValue

case class NumericValue(value: BigDecimal) extends ParameterValue

case class StringValue(value: String) extends ParameterValue

case class TimestampValue(millis: Long) extends ParameterValue

object ParameterValue {
  val TYPE_N: Byte = 'n'
  val TYPE_S: Byte = 's'
  val TYPE_T: Byte = 't'

  implicit val pvRw: ReadWrite[ParameterValue] = new ReadWrite[ParameterValue] {
    override def read[B](buf: B)(implicit B: Buffer[B]): ParameterValue = {
      B.readByte(buf) match {
        case TYPE_N => NumericValue(implicitly[ReadWrite[BigDecimal]].read(buf))
        case TYPE_S => StringValue(implicitly[ReadWrite[String]].read(buf))
        case TYPE_T => TimestampValue(implicitly[ReadWrite[Long]].read(buf))
        case x      => throw new IllegalArgumentException(s"Unsupported type '$x''")
      }
    }

    override def write[B: Buffer](buf: B, t: ParameterValue): Unit = ???
  }
}
