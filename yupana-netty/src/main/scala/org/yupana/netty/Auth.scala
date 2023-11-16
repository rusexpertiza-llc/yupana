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

import io.netty.buffer.ByteBuf
import org.yupana.protocol._

class Auth(serverContext: ServerContext) extends ConnectionState {
  import NettyBuffer._
  override def init(): Seq[Response[_]] = Seq(CredentialsRequest(CredentialsRequest.METHOD_PLAIN))

  override def extractCommand(frame: Frame): Either[ErrorMessage, Option[Command[_]]] = {
    frame.frameType match {
      case Tags.CREDENTIALS => Right(Some(Credentials.readFrame[ByteBuf](frame)))
      case x                => Left(ErrorMessage(s"Unexpected command type '$x'"))
    }
  }

  override def processCommand(command: Command[_]): (ConnectionState, Seq[Response[_]]) = {
    command match {
      case Credentials(CredentialsRequest.METHOD_PLAIN, u, p) => (new Ready(serverContext), Seq(Authorized()))
      case Credentials(m, _, _) => (this, Seq(ErrorMessage(s"Unsupported auth method $m")))
      case _                    => throw new IllegalStateException(s"Unexpected command $command")
    }
  }
}
