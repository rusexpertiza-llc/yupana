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
  * Batch query
  * @param id request id
  * @param query query text
  * @param params list of params
  */
case class BatchQuery(id: Int, query: String, params: Seq[Map[Int, ParameterValue]])
    extends Command[BatchQuery](BatchQuery)

object BatchQuery extends MessageHelper[BatchQuery] {

  override val tag: Byte = Tags.BATCH_QUERY
  override val readWrite: ReadWrite[BatchQuery] =
    ReadWrite.product3[BatchQuery, Int, String, Seq[Map[Int, ParameterValue]]](BatchQuery.apply)(q =>
      (q.id, q.query, q.params)
    )
}
