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

class Ready(serverContext: ServerContext) extends ConnectionState {
  override def init(): Seq[Response[_]] = Seq(Idle())

  override def extractCommand(frame: Frame): Either[ErrorMessage, Option[Command[_]]] = {
    frame.frameType match {
      case Tags.PREPARE_QUERY =>
        PrepareQuery.readFrameOpt(frame).toRight(ErrorMessage("")).map(Some(_))

      case x => Left(ErrorMessage(s"Unexpected command '${x.toChar}'"))
    }
  }

  override def processCommand(command: Command[_]): (ConnectionState, Seq[Response[_]]) = {
    command match {
      case pq: PrepareQuery =>
        serverContext.requestHandler.handleQuery(pq) match {
          case Right(iter) => ???
          case Left(msg)   => (new Ready(serverContext), Seq(ErrorMessage(msg)))
        }

      case _ => throw new IllegalStateException(s"Unexpected command $command")
    }
  }
}
