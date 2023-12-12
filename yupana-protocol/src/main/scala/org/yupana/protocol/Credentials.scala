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
  * Credentials response from the client
  * @param method authentication method
  * @param user user name
  * @param password password
  */
case class Credentials(method: String, user: String, password: String) extends Command[Credentials](Credentials)

object Credentials extends MessageHelper[Credentials] {
  override val tag: Byte = Tags.CREDENTIALS
  override val readWrite: ReadWrite[Credentials] =
    ReadWrite.product3[Credentials, String, String, String](Credentials.apply)(c => (c.method, c.user, c.password))
}
