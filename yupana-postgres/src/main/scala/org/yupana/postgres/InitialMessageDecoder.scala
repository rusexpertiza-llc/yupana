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

import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import org.yupana.postgres.protocol.{ Message, SSLRequest, StartupMessage }

import java.nio.charset.{ Charset, StandardCharsets }
import java.util

class InitialMessageDecoder extends ReplayingDecoder[Message] with StrictLogging {

  def readNullTerminatedString(in: ByteBuf, charset: Charset): String = {
    val toEnd = in.bytesBefore(0.toByte)
    val res = in.readCharSequence(toEnd, charset).toString
    in.readByte()
    res
  }

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val len = in.readInt()
    println(s"GOT MESSAGE $len")
    val v = in.readInt()

    if (v == 80877103) {
      logger.info(s"Got SSL Request from ${ctx.channel().remoteAddress()}")
      println(s"GOT SSL Request")
      out.add(SSLRequest)
    } else {
      val vMaj = v >> 16
      val vMin = v & 0xFF
      println(s"StartupMessage, $vMaj.$vMin")

      var params = Map.empty[String, String]

      while (in.isReadable && in.readerIndex() < len) {
        val k = readNullTerminatedString(in, StandardCharsets.US_ASCII)
        if (k.nonEmpty) {
          val v = readNullTerminatedString(in, StandardCharsets.US_ASCII)
          params += k -> v
        }
      }
      println(s"PARAMS: $params")
      out.add(StartupMessage(params("user")))
    }
  }
}
