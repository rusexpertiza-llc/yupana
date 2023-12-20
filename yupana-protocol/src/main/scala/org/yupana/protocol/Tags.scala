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

object Tags extends Enumeration {
  type Tags = Value

  final protected case class TagVal(value: Byte) extends super.Val

  import scala.language.implicitConversions

  implicit def valueToPlanetVal(x: Value): TagVal = x.asInstanceOf[TagVal]

  val ERROR_MESSAGE: TagVal = TagVal('E')
  val HEARTBEAT: TagVal = TagVal('k')
  val QUIT: TagVal = TagVal('q')

  val CANCEL: TagVal = TagVal('x')
  val CANCELED: TagVal = TagVal('X')

  val HELLO: TagVal = TagVal('h')
  val HELLO_RESPONSE: TagVal = TagVal('H')

  val CREDENTIALS: TagVal = TagVal('c')
  val CREDENTIALS_REQUEST: TagVal = TagVal('C')
  val AUTHORIZED: TagVal = TagVal('A')

  val SQL_QUERY: TagVal = TagVal('s')
  val BATCH_QUERY: TagVal = TagVal('b')
  val NEXT: TagVal = TagVal('n')

  val RESULT_HEADER: TagVal = TagVal('R')
  val RESULT_ROW: TagVal = TagVal('D')
  val RESULT_FOOTER: TagVal = TagVal('F')
}
