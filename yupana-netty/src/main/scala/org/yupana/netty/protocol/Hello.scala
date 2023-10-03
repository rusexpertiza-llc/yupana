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

import java.nio.charset.StandardCharsets

trait Command

trait CommandHelper[C <: Command] {
  val tag: Byte
  def encode(c: C, out: ByteBuf): Unit
  def decode(b: ByteBuf): C
}

case class Hello(protocolVersion: Int, clientVersion: String, params: Map[String, String]) extends Command

object Hello extends CommandHelper[Hello] {
  override val tag: Byte = 1

  implicit val rw: ReadWrite[Hello] = new ReadWrite[Hello] {
    override def read(buf: ByteBuf): Hello = {
      Hello(buf.readInt(), implicitly[ReadWrite[String]].read(buf), Map.empty)
    }

    override def write(buf: ByteBuf, t: Hello): Unit = {
      buf.writeInt(t.protocolVersion)
      implicitly[ReadWrite[String]].write(buf, t.clientVersion)
    }
  }

  override def encode(c: Hello, out: ByteBuf): Unit = {
    out.writeByte(tag).writeInt(4).writeBytes("version".getBytes(StandardCharsets.UTF_8))
  }

  override def decode(b: ByteBuf): Hello = ???
}

object Command {}
