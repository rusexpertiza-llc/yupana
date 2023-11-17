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

  override def handleFrame(frame: Frame): Either[ErrorMessage, (ConnectionState, Seq[Response[_]])] = {
    frame.frameType match {
      case Tags.PREPARE_QUERY =>
        PrepareQuery.readFrameSafe(frame).left.map(ErrorMessage(_)).map(handleQuery)

      case x => Left(ErrorMessage(s"Unexpected command '${x.toChar}'"))
    }
  }

  private def handleQuery(command: PrepareQuery): (ConnectionState, Seq[Response[_]]) = {
    command match {
      case pq: PrepareQuery =>
        serverContext.requestHandler.handleQuery(pq) match {
          case Right(iter) => (new Ready(serverContext), iter.toSeq)
          case Left(msg)   => (new Ready(serverContext), Seq(ErrorMessage(msg)))
        }
    }
  }
}
