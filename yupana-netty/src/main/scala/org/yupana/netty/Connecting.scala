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

package org.yupana.netty

import org.yupana.protocol._

class Connecting(serverContext: ServerContext) extends ConnectionState {
  import NettyBuffer._

  override def init(): Seq[Response[_]] = Nil

  override def extractCommand(frame: Frame): Either[ErrorMessage, Option[Command[_]]] = {
    Hello.readFrameOpt(frame).toRight(ErrorMessage("Expect Hello")).map(Some(_))
  }

  override def processCommand(command: Command[_]): (ConnectionState, Seq[Response[_]]) = {
    command match {
      case Hello(pv, _, time, _) if pv == ProtocolVersion.value =>
        (new Auth(serverContext), Seq(HelloResponse(ProtocolVersion.value, time)))
      case Hello(pv, _, _, _) =>
        (this, Seq(ErrorMessage(s"Unsupported protocol version $pv, required ${ProtocolVersion.value}")))
      case _ => throw new IllegalAccessException(s"Unexpected command $command")
    }
  }
}
