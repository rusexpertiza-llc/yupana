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
  * Authentication request
  * @param methods supported authentication methods
  */
case class CredentialsRequest(methods: Seq[String]) extends Response[CredentialsRequest](CredentialsRequest)

object CredentialsRequest extends MessageHelper[CredentialsRequest] {
  val METHOD_PLAIN: String = "plain"

  override val tag: Tag = CredentialsRequestTag
  override val readWrite: ReadWrite[CredentialsRequest] =
    ReadWrite[Seq[String]].imap(CredentialsRequest.apply)(_.methods)
}
