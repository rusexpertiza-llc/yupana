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
  override def init(): Seq[Response[_]] = Seq(CredentialsRequest(serverContext.authorizer.method))

  override def handleFrame(frame: Frame): Either[ErrorMessage, (ConnectionState, Seq[Response[_]])] = {
    frame.frameType match {
      case Tags.CREDENTIALS => handleCredentials(Credentials.readFrame[ByteBuf](frame))
      case x                => Left(ErrorMessage(s"Unexpected command type '$x'"))
    }
  }

  private def handleCredentials(command: Credentials): Either[ErrorMessage, (ConnectionState, Seq[Response[_]])] = {
    command match {
      case Credentials(m, u, p) =>
        serverContext.authorizer
          .authorize(m, u, p)
          .fold(
            err => Left(ErrorMessage(err, ErrorMessage.SEVERITY_FATAL)),
            user => Right((new Ready(serverContext.copy(user = Some(user))), Seq(Authorized())))
          )
    }
  }
}
