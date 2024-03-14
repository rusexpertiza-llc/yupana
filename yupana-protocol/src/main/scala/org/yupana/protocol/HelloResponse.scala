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

import org.yupana.serialization.ReadWrite

/**
  * Server greeting to the client
  *
  * @param protocolVersion server protocol version
  * @param reqTime client request time from [[Hello]]
  */
case class HelloResponse(protocolVersion: Int, reqTime: Long) extends Response[HelloResponse](HelloResponse)

object HelloResponse extends MessageHelper[HelloResponse] {
  override val tag: Tags.Tags = Tags.HELLO_RESPONSE
  override val readWrite: ReadWrite[HelloResponse] =
    ReadWrite.product2[HelloResponse, Int, Long](HelloResponse.apply)(x => (x.protocolVersion, x.reqTime))
}
