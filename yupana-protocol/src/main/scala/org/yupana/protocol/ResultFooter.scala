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
  * Query execution statistics
  * @param id request id
  * @param millis execution time
  * @param rows number of rows provided to the client
  */
case class ResultFooter(id: Int, millis: Long, rows: Int) extends Response[ResultFooter](ResultFooter)

object ResultFooter extends MessageHelper[ResultFooter] {
  override val tag: Byte = Tags.RESULT_FOOTER
  override val readWrite: ReadWrite[ResultFooter] =
    ReadWrite.product3[ResultFooter, Int, Long, Int](ResultFooter.apply)(f => (f.id, f.millis, f.rows))
}
