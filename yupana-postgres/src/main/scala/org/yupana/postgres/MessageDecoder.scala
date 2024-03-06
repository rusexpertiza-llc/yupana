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

package org.yupana.postgres

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import org.yupana.postgres.protocol.{ ClientMessage, SimpleQuery }

import java.nio.charset.Charset
import java.util

class MessageDecoder(charset: Charset) extends ReplayingDecoder[ClientMessage] {

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val tag = in.readByte()
    val size = in.readInt()

    println(s"GOT ${tag.toChar} $size b")
    tag match {
      case 'Q' =>
        val q = NettyUtils.readNullTerminatedString(in, charset)
        println(s"GOT Q ${q}")
        out.add(SimpleQuery(q))
    }
  }
}
