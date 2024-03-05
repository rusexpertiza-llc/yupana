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

import io.netty.buffer.ByteBuf

trait Message

trait ClientMessage extends Message

case object SSLRequest extends ClientMessage

case class StartupMessage(user: String) extends ClientMessage

trait ServerMessage extends Message {
  def write(byteBuf: ByteBuf): Unit
}

case object No extends ServerMessage {
  override def write(byteBuf: ByteBuf): Unit = byteBuf.writeByte('N'.toByte)
}

case object AuthClearTextPassword extends ServerMessage {
  override def write(byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte('R')
    byteBuf.writeInt(4 + 4)
    byteBuf.writeInt(3)
  }
}

case object AuthOk extends ServerMessage {
  override def write(byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte('R')
    byteBuf.writeInt(4 + 4)
    byteBuf.writeInt(0)
  }
}

case object ReadyForQuery extends ServerMessage {
  override def write(byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte('Z')
    byteBuf.writeInt(4 + 1)
    byteBuf.writeByte('I'.toByte)
  }
}
