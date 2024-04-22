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

package org.yupana.postgres.protocol

import io.netty.buffer.{ ByteBuf, Unpooled }

import java.nio.charset.Charset

trait Message

trait ClientMessage extends Message

trait ServerMessage extends Message {
  def write(byteBuf: ByteBuf, charset: Charset): Unit
}

trait TaggedServerMessage extends ServerMessage {

  val tag: Byte
  override def write(byteBuf: ByteBuf, charset: Charset): Unit = {
    byteBuf.writeByte(tag)
    val payload = Unpooled.buffer()
    writePayload(payload, charset)
    byteBuf.writeInt(payload.readableBytes() + 4)
    byteBuf.writeBytes(payload)
  }

  def writePayload(buf: ByteBuf, charset: Charset): Unit
}
