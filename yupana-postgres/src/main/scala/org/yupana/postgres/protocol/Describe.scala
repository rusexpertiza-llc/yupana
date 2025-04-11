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
import org.yupana.postgres.NettyUtils
import org.yupana.postgres.protocol.Describe.DescribeType

import java.nio.charset.Charset

case class Describe(variant: DescribeType, name: String) extends ClientMessage

object Describe {

  sealed trait DescribeType
  case object DescribeStatement extends DescribeType
  case object DescribePortal extends DescribeType

  def decode(in: ByteBuf, charset: Charset): Either[String, Describe] = {
    val variant = in.readByte()
    val name = NettyUtils.readNullTerminatedString(in, charset)
    variant match {
      case 'S' => Right(Describe(DescribeStatement, name))
      case 'P' => Right(Describe(DescribePortal, name))
      case x   => Left(s"Unsupported describe type '${x.toChar}'")
    }
  }
}
