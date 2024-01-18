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

import org.yupana.protocol.CredentialsRequest

object NonEmptyUserAuthorizer extends Authorizer {
  override val methods: Seq[String] = Seq(CredentialsRequest.METHOD_PLAIN)

  override def authorize(method: String, userName: String, password: String): Either[String, String] = {
    if (method == CredentialsRequest.METHOD_PLAIN) {
      val fixedName = userName.trim
      Either.cond(fixedName.nonEmpty, fixedName, "Username should not be empty")
    } else {
      Left(s"Unsupported auth method '$method'")
    }
  }
}