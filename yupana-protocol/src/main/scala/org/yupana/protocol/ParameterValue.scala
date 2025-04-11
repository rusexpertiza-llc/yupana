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

import org.yupana.api.types.ByteReaderWriter

sealed trait ParameterValue

case class NumericValue(value: BigDecimal) extends ParameterValue

case class StringValue(value: String) extends ParameterValue

case class TimestampValue(millis: Long) extends ParameterValue

object ParameterValue {
  val TYPE_N: Byte = 'n'
  val TYPE_S: Byte = 's'
  val TYPE_T: Byte = 't'

  implicit val pvRw: ReadWrite[ParameterValue] = new ReadWrite[ParameterValue] {
    override def read[B](buf: B)(implicit B: ByteReaderWriter[B]): ParameterValue = {
      B.readByte(buf) match {
        case TYPE_N => NumericValue(implicitly[ReadWrite[BigDecimal]].read(buf))
        case TYPE_S => StringValue(implicitly[ReadWrite[String]].read(buf))
        case TYPE_T => TimestampValue(implicitly[ReadWrite[Long]].read(buf))
        case x      => throw new IllegalArgumentException(s"Unsupported type '$x''")
      }
    }

    override def write[B: ByteReaderWriter](buf: B, t: ParameterValue): Unit = {
      t match {
        case NumericValue(value)   => write(buf, TYPE_N, value)
        case StringValue(value)    => write(buf, TYPE_S, value)
        case TimestampValue(value) => write(buf, TYPE_T, value)
      }
    }

    private def write[T, B](buf: B, tag: Byte, t: T)(implicit b: ByteReaderWriter[B], tw: ReadWrite[T]): Unit = {
      b.writeByte(buf, tag)
      tw.write(buf, t)
    }
  }
}
