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
  * Protocol messages tags.  Client messages use lower case chars, server messages use upper case.
  */
sealed trait Tag {
  val value: Byte

  def toChar: Char = value.toChar
}

case object ErrorMessageTag extends Tag { override val value: Byte = 'E' }
case object HeartbeatTag extends Tag { override val value: Byte = 'k' }

case object QuitTag extends Tag { override val value: Byte = 'q' }

case object CancelTag extends Tag { override val value: Byte = 'x' }
case object CanceledTag extends Tag { override val value: Byte = 'X' }

case object HelloTag extends Tag { override val value: Byte = 'h' }
case object HelloResponseTag extends Tag { override val value: Byte = 'H' }

case object CredentialsRequestTag extends Tag { override val value: Byte = 'C' }
case object CredentialsTag extends Tag { override val value: Byte = 'c' }
case object AuthorizedTag extends Tag { override val value: Byte = 'A' }

case object SqlQueryTag extends Tag { override val value: Byte = 's' }
case object BatchQueryTag extends Tag { override val value: Byte = 'b' }
case object NextTag extends Tag { override val value: Byte = 'n' }

case object ResultHeaderTag extends Tag { override val value: Byte = 'R' }
case object ResultRowTag extends Tag { override val value: Byte = 'D' }
case object ResultFooterTag extends Tag { override val value: Byte = 'F' }
