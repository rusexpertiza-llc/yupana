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

object Tags {
  val ERROR_MESSAGE: Byte = 'E'
  val HEARTBEAT: Byte = 'B'

  val CANCEL: Byte = 'x'
  val CANCELED: Byte = 'X'

  val HELLO: Byte = 'h'
  val HELLO_RESPONSE: Byte = 'H'

  val BATCH_QUERY: Byte = 'b'
  val PREPARE_QUERY: Byte = 'p'
  val NEXT: Byte = 'n'

  val RESULT_HEADER: Byte = 'R'
  val RESULT_ROW: Byte = 'D'
  val RESULT_FOOTER: Byte = 'F'

  val CREDENTIALS_REQUEST: Byte = 'C'
  val CREDENTIALS: Byte = 'c'
  val AUTHORIZED: Byte = 'A'
}
