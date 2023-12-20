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

package org.yupana.protocol

/**
  * Error message from server to client
  * @param message text of the message
  * @param streamId if the error related to some query
  * @param severity message importance
  */
case class ErrorMessage(message: String, streamId: Option[Int] = None, severity: Byte = ErrorMessage.SEVERITY_ERROR)
    extends Response[ErrorMessage](ErrorMessage)

object ErrorMessage extends MessageHelper[ErrorMessage] {
  override val tag: Tag = ErrorMessageTag

  val SEVERITY_FATAL: Byte = -1
  val SEVERITY_ERROR: Byte = 0

  implicit override val readWrite: ReadWrite[ErrorMessage] =
    ReadWrite.product3[ErrorMessage, Option[Int], Byte, String]((id, s, m) => ErrorMessage(m, id, s))(e =>
      (e.streamId, e.severity, e.message)
    )
}
