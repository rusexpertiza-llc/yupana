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

package org.yupana.netty.protocol
import io.netty.buffer.ByteBuf

case class ErrorMessage(message: String) extends Response[ErrorMessage](ErrorMessage)

object ErrorMessage extends MessageHelper[ErrorMessage] {
  override val tag: Byte = Tags.ERROR_MESSAGE

  implicit override val readWrite: ReadWrite[ErrorMessage] = new ReadWrite[ErrorMessage] {
    override def read(buf: ByteBuf): ErrorMessage = ErrorMessage(implicitly[ReadWrite[String]].read(buf))
    override def write(buf: ByteBuf, t: ErrorMessage): Unit = implicitly[ReadWrite[String]].write(buf, t.message)
  }
}
case class HelloResponse(protocolVersion: Int) extends Response[HelloResponse](HelloResponse)

object HelloResponse extends MessageHelper[HelloResponse] {
  override val tag: Byte = Tags.HELLO_RESPONSE
  override val readWrite: ReadWrite[HelloResponse] = new ReadWrite[HelloResponse] {
    override def read(buf: ByteBuf): HelloResponse = HelloResponse(implicitly[ReadWrite[Int]].read(buf))
    override def write(buf: ByteBuf, t: HelloResponse): Unit = implicitly[ReadWrite[Int]].write(buf, t.protocolVersion)
  }
}

object Tags {
  val HELLO: Byte = 0x01.toByte
  val HELLO_RESPONSE: Byte = 0x81.toByte
  val ERROR_MESSAGE: Byte = 0xFF.toByte
}
